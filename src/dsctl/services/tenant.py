from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias, TypedDict

from dsctl.cli_surface import TENANT_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConflictError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._serialization import TenantData, optional_text, serialize_tenant
from dsctl.services._validation import (
    require_delete_force,
    require_non_empty_text,
    require_positive_int,
)
from dsctl.services.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_AUTO_EXHAUST_PAGES,
    PageData,
    requested_page_data,
)
from dsctl.services.resolver import ResolvedTenantData
from dsctl.services.resolver import queue as resolve_queue
from dsctl.services.resolver import tenant as resolve_tenant
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime

if TYPE_CHECKING:
    from dsctl.upstream.protocol import TenantRecord


TenantPageData: TypeAlias = PageData[TenantData]

REQUEST_PARAMS_NOT_VALID_ERROR = 10001
OS_TENANT_CODE_EXIST = 10009
TENANT_NOT_EXIST = 10017
DELETE_TENANT_BY_ID_FAIL = 10142
DELETE_TENANT_BY_ID_FAIL_DEFINES = 10143
DELETE_TENANT_BY_ID_FAIL_USERS = 10144
CHECK_OS_TENANT_CODE_ERROR = 10164
QUEUE_NOT_EXIST = 10128
USER_NO_OPERATION_PERM = 30001
DESCRIPTION_TOO_LONG_ERROR = 1400004
TENANT_FULL_NAME_TOO_LONG_ERROR = 1300016


class DeleteTenantData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    tenant: ResolvedTenantData


class _UnsetValue:
    """Sentinel for update fields that should keep their current value."""


UNSET = _UnsetValue()
DescriptionUpdate = str | None | _UnsetValue


def list_tenants_result(
    *,
    env_file: str | None = None,
    search: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
) -> CommandResult:
    """List tenants with explicit paging or auto-exhaust support."""
    normalized_search = optional_text(search)
    require_positive_int(page_no, label="page_no")
    require_positive_int(page_size, label="page_size")

    return run_with_service_runtime(
        env_file,
        _list_tenants_result,
        search=normalized_search,
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
    )


def get_tenant_result(
    tenant: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Resolve and fetch one tenant payload."""
    return run_with_service_runtime(
        env_file,
        _get_tenant_result,
        tenant=tenant,
    )


def create_tenant_result(
    *,
    tenant_code: str,
    queue: str,
    description: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create one tenant from validated CLI input."""
    normalized_tenant_code = require_non_empty_text(
        tenant_code,
        label="tenant code",
    )
    normalized_queue = require_non_empty_text(queue, label="queue")
    normalized_description = optional_text(description)

    return run_with_service_runtime(
        env_file,
        _create_tenant_result,
        tenant_code=normalized_tenant_code,
        queue=normalized_queue,
        description=normalized_description,
    )


def update_tenant_result(
    tenant: str,
    *,
    tenant_code: str | None = None,
    queue: str | None = None,
    description: DescriptionUpdate = UNSET,
    env_file: str | None = None,
) -> CommandResult:
    """Update one tenant while preserving omitted fields."""
    if tenant_code is None and queue is None and description is UNSET:
        message = "Tenant update requires at least one field change"
        raise UserInputError(
            message,
            suggestion=(
                "Pass at least one update flag such as --tenant-code, --queue, "
                "or --description."
            ),
        )

    normalized_tenant_code = (
        require_non_empty_text(tenant_code, label="tenant code")
        if tenant_code is not None
        else None
    )
    normalized_queue = (
        require_non_empty_text(queue, label="queue") if queue is not None else None
    )
    normalized_description = (
        optional_text(description)
        if not isinstance(description, _UnsetValue)
        else UNSET
    )

    return run_with_service_runtime(
        env_file,
        _update_tenant_result,
        tenant=tenant,
        tenant_code=normalized_tenant_code,
        queue=normalized_queue,
        description=normalized_description,
    )


def delete_tenant_result(
    tenant: str,
    *,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one tenant after explicit confirmation."""
    require_delete_force(force=force, resource_label="Tenant")

    return run_with_service_runtime(
        env_file,
        _delete_tenant_result,
        tenant=tenant,
    )


def _list_tenants_result(
    runtime: ServiceRuntime,
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.upstream.tenants
    data: TenantPageData = requested_page_data(
        lambda current_page_no, current_page_size: adapter.list(
            page_no=current_page_no,
            page_size=current_page_size,
            search=search,
        ),
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=serialize_tenant,
        resource=TENANT_RESOURCE,
        max_pages=MAX_AUTO_EXHAUST_PAGES,
        translate_error=lambda error: _translate_tenant_api_error(
            error,
            operation="list",
            tenant_code=search,
        ),
    )

    return CommandResult(
        data=require_json_object(data, label="tenant list data"),
        resolved={
            "search": search,
            "page_no": page_no,
            "page_size": page_size,
            "all": all_pages,
        },
    )


def _get_tenant_result(
    runtime: ServiceRuntime,
    *,
    tenant: str,
) -> CommandResult:
    adapter = runtime.upstream.tenants
    resolved_tenant = resolve_tenant(tenant, adapter=adapter)
    fetched_tenant = adapter.get(tenant_id=resolved_tenant.id)
    return CommandResult(
        data=require_json_object(
            serialize_tenant(fetched_tenant),
            label="tenant data",
        ),
        resolved={
            "tenant": require_json_object(
                resolved_tenant.to_data(),
                label="resolved tenant",
            )
        },
    )


def _create_tenant_result(
    runtime: ServiceRuntime,
    *,
    tenant_code: str,
    queue: str,
    description: str | None,
) -> CommandResult:
    tenant_adapter = runtime.upstream.tenants
    resolved_queue = resolve_queue(queue, adapter=runtime.upstream.queues)
    try:
        created_tenant = tenant_adapter.create(
            tenant_code=tenant_code,
            queue_id=resolved_queue.id,
            description=description,
        )
    except ApiResultError as error:
        raise _translate_tenant_api_error(
            error,
            operation="create",
            tenant_code=tenant_code,
            queue_id=resolved_queue.id,
        ) from error

    return CommandResult(
        data=require_json_object(
            serialize_tenant(created_tenant),
            label="tenant data",
        ),
        resolved={
            "tenant": require_json_object(
                _resolved_tenant_data_from_record(created_tenant),
                label="resolved tenant",
            )
        },
    )


def _update_tenant_result(
    runtime: ServiceRuntime,
    *,
    tenant: str,
    tenant_code: str | None,
    queue: str | None,
    description: DescriptionUpdate,
) -> CommandResult:
    tenant_adapter = runtime.upstream.tenants
    resolved_tenant = resolve_tenant(tenant, adapter=tenant_adapter)
    current_tenant = tenant_adapter.get(tenant_id=resolved_tenant.id)
    next_tenant_code = current_tenant.tenantCode if tenant_code is None else tenant_code
    if next_tenant_code is None:
        message = "Tenant payload was missing required fields"
        raise ApiTransportError(
            message,
            details={"resource": TENANT_RESOURCE, "id": resolved_tenant.id},
        )

    next_queue_id = current_tenant.queueId
    if queue is not None:
        resolved_queue = resolve_queue(queue, adapter=runtime.upstream.queues)
        next_queue_id = resolved_queue.id

    next_description = (
        current_tenant.description
        if isinstance(description, _UnsetValue)
        else description
    )

    if (
        next_tenant_code == current_tenant.tenantCode
        and next_queue_id == current_tenant.queueId
        and next_description == current_tenant.description
    ):
        message = "Tenant update requires at least one field change"
        raise UserInputError(
            message,
            suggestion=(
                "Pass a different --tenant-code, --queue, or --description value."
            ),
        )

    try:
        updated_tenant = tenant_adapter.update(
            tenant_id=resolved_tenant.id,
            tenant_code=next_tenant_code,
            queue_id=next_queue_id,
            description=next_description,
        )
    except ApiResultError as error:
        raise _translate_tenant_api_error(
            error,
            operation="update",
            tenant_id=resolved_tenant.id,
            tenant_code=next_tenant_code,
            queue_id=next_queue_id,
        ) from error

    return CommandResult(
        data=require_json_object(
            serialize_tenant(updated_tenant),
            label="tenant data",
        ),
        resolved={
            "tenant": require_json_object(
                resolved_tenant.to_data(),
                label="resolved tenant",
            )
        },
    )


def _delete_tenant_result(
    runtime: ServiceRuntime,
    *,
    tenant: str,
) -> CommandResult:
    tenant_adapter = runtime.upstream.tenants
    resolved_tenant = resolve_tenant(tenant, adapter=tenant_adapter)
    try:
        deleted = tenant_adapter.delete(tenant_id=resolved_tenant.id)
    except ApiResultError as error:
        raise _translate_tenant_api_error(
            error,
            operation="delete",
            tenant_id=resolved_tenant.id,
            tenant_code=resolved_tenant.tenant_code,
            queue_id=resolved_tenant.queue_id,
        ) from error

    data: DeleteTenantData = {
        "deleted": deleted,
        "tenant": resolved_tenant.to_data(),
    }
    return CommandResult(
        data=require_json_object(data, label="tenant delete data"),
        resolved={
            "tenant": require_json_object(
                resolved_tenant.to_data(),
                label="resolved tenant",
            )
        },
    )


def _resolved_tenant_data_from_record(tenant: TenantRecord) -> ResolvedTenantData:
    tenant_id = tenant.id
    tenant_code = tenant.tenantCode
    if tenant_id is None or tenant_code is None:
        message = "Tenant payload was missing required identity fields"
        raise ApiTransportError(
            message,
            details={"resource": TENANT_RESOURCE},
        )
    return {
        "id": tenant_id,
        "tenantCode": tenant_code,
        "description": tenant.description,
        "queueId": tenant.queueId,
        "queueName": tenant.queueName,
        "queue": tenant.queue,
    }


def _translate_tenant_api_error(
    error: ApiResultError,
    *,
    operation: str,
    tenant_id: int | None = None,
    tenant_code: str | None = None,
    queue_id: int | None = None,
) -> Exception:
    details: dict[str, str | int] = {"operation": operation}
    if tenant_id is not None:
        details["id"] = tenant_id
    if tenant_code is not None:
        details["tenantCode"] = tenant_code
    if queue_id is not None:
        details["queueId"] = queue_id

    if error.result_code == TENANT_NOT_EXIST:
        identifier = tenant_id if tenant_id is not None else tenant_code
        message = f"Tenant {identifier!r} was not found"
        return NotFoundError(message, details=details)
    if error.result_code == QUEUE_NOT_EXIST:
        message = f"Queue {queue_id!r} was not found"
        return NotFoundError(message, details=details)
    if error.result_code == OS_TENANT_CODE_EXIST:
        message = "Tenant create/update conflicted with an existing tenant code"
        return ConflictError(message, details=details)
    if error.result_code in (
        DELETE_TENANT_BY_ID_FAIL,
        DELETE_TENANT_BY_ID_FAIL_DEFINES,
        DELETE_TENANT_BY_ID_FAIL_USERS,
    ):
        message = "Tenant is still in use and cannot be deleted"
        return ConflictError(message, details=details)
    if error.result_code == USER_NO_OPERATION_PERM:
        message = f"Tenant {operation} requires additional permissions"
        return PermissionDeniedError(message, details=details)
    if error.result_code in (
        REQUEST_PARAMS_NOT_VALID_ERROR,
        CHECK_OS_TENANT_CODE_ERROR,
        DESCRIPTION_TOO_LONG_ERROR,
        TENANT_FULL_NAME_TOO_LONG_ERROR,
    ):
        message = "Tenant input was rejected by the upstream API"
        return UserInputError(
            message,
            details=details,
            suggestion=(
                "Verify the tenant code, queue, and description values, then "
                "retry the same tenant command."
            ),
        )
    return error
