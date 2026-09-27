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
from dsctl.services._page_result import paged_command_result
from dsctl.services._validation import (
    require_delete_force,
    require_non_empty_text,
    require_positive_int,
)
from dsctl.services.runtime import (
    BoundDomainServiceRuntime,
    run_with_bound_domain_service_runtime,
)
from dsctl.upstream.pagination import DEFAULT_PAGE_SIZE
from dsctl.upstream.resolver import ResolvedTenantData
from dsctl.upstream.resolver import queue as resolve_queue
from dsctl.upstream.resolver import tenant as resolve_tenant
from dsctl.upstream.serialization import optional_text, serialize_tenant
from dsctl.upstream.tenants import TENANT_DOMAIN, TenantDomain

if TYPE_CHECKING:
    from dsctl.upstream.protocol import TenantRecord
    from dsctl.upstream.resolver import ResolvedQueue


TenantServiceRuntime: TypeAlias = BoundDomainServiceRuntime[TenantDomain]

REQUEST_PARAMS_NOT_VALID_ERROR = 10001
OS_TENANT_CODE_EXIST = 10009
TENANT_NOT_EXIST = 10017
VERIFY_TENANT_CODE_ERROR = 10089
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

    return run_with_bound_domain_service_runtime(
        env_file,
        TENANT_DOMAIN,
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
    return run_with_bound_domain_service_runtime(
        env_file,
        TENANT_DOMAIN,
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

    return run_with_bound_domain_service_runtime(
        env_file,
        TENANT_DOMAIN,
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
    """Update mutable tenant fields while preserving its immutable code."""
    if tenant_code is None and queue is None and description is UNSET:
        message = "Tenant update requires at least one field change"
        raise UserInputError(
            message,
            suggestion=(
                "Pass at least one update flag such as --queue, --description, "
                "or --clear-description."
            ),
        )

    normalized_queue = (
        require_non_empty_text(queue, label="queue") if queue is not None else None
    )
    normalized_tenant_code = (
        require_non_empty_text(tenant_code, label="tenant code")
        if tenant_code is not None
        else None
    )
    normalized_description = (
        optional_text(description)
        if not isinstance(description, _UnsetValue)
        else UNSET
    )

    return run_with_bound_domain_service_runtime(
        env_file,
        TENANT_DOMAIN,
        _update_tenant_result,
        tenant=tenant,
        requested_tenant_code=normalized_tenant_code,
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

    return run_with_bound_domain_service_runtime(
        env_file,
        TENANT_DOMAIN,
        _delete_tenant_result,
        tenant=tenant,
    )


def _list_tenants_result(
    runtime: TenantServiceRuntime,
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.domain.tenants
    return paged_command_result(
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
        resolved={"search": search},
        translate_error=lambda error: _translate_tenant_api_error(
            error,
            ds_version=runtime.profile.ds_version,
            operation="list",
            tenant_code=search,
        ),
    )


def _get_tenant_result(
    runtime: TenantServiceRuntime,
    *,
    tenant: str,
) -> CommandResult:
    adapter = runtime.domain.tenants
    try:
        resolved_tenant = resolve_tenant(tenant, adapter=adapter)
        fetched_tenant = adapter.get(tenant_id=resolved_tenant.id)
    except ApiResultError as error:
        raise _translate_tenant_api_error(
            error,
            ds_version=runtime.profile.ds_version,
            operation="get",
            tenant_code=tenant,
        ) from error
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
    runtime: TenantServiceRuntime,
    *,
    tenant_code: str,
    queue: str,
    description: str | None,
) -> CommandResult:
    tenant_adapter = runtime.domain.tenants
    resolved_queue = _resolve_tenant_queue(
        queue,
        runtime=runtime,
        operation="create",
    )
    try:
        created_tenant = tenant_adapter.create(
            tenant_code=tenant_code,
            queue_id=resolved_queue.id,
            description=description,
        )
    except ApiResultError as error:
        raise _translate_tenant_api_error(
            error,
            ds_version=runtime.profile.ds_version,
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
    runtime: TenantServiceRuntime,
    *,
    tenant: str,
    requested_tenant_code: str | None,
    queue: str | None,
    description: DescriptionUpdate,
) -> CommandResult:
    tenant_adapter = runtime.domain.tenants
    try:
        resolved_tenant = resolve_tenant(tenant, adapter=tenant_adapter)
    except ApiResultError as error:
        raise _translate_tenant_api_error(
            error,
            ds_version=runtime.profile.ds_version,
            operation="update",
            tenant_code=tenant,
        ) from error

    if (
        requested_tenant_code is not None
        and requested_tenant_code != resolved_tenant.tenant_code
    ):
        message = "Tenant code is immutable and cannot be changed"
        raise UserInputError(
            message,
            suggestion=(
                f"Keep --tenant-code {resolved_tenant.tenant_code} for backward "
                "compatibility, or create a new tenant with the desired code."
            ),
        )

    next_queue_id = resolved_tenant.queue_id
    if queue is not None:
        resolved_queue = _resolve_tenant_queue(
            queue,
            runtime=runtime,
            operation="update",
        )
        next_queue_id = resolved_queue.id

    next_description = (
        resolved_tenant.description
        if isinstance(description, _UnsetValue)
        else description
    )

    if (
        next_queue_id == resolved_tenant.queue_id
        and next_description == resolved_tenant.description
    ):
        message = "Tenant update requires at least one field change"
        raise UserInputError(
            message,
            suggestion=(
                "Pass a different --queue or --description value, or use "
                "--clear-description."
            ),
        )

    try:
        updated_tenant = tenant_adapter.update(
            tenant_id=resolved_tenant.id,
            current_tenant_code=resolved_tenant.tenant_code,
            queue_id=next_queue_id,
            description=next_description,
        )
    except ApiResultError as error:
        raise _translate_tenant_api_error(
            error,
            ds_version=runtime.profile.ds_version,
            operation="update",
            tenant_id=resolved_tenant.id,
            tenant_code=resolved_tenant.tenant_code,
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
    runtime: TenantServiceRuntime,
    *,
    tenant: str,
) -> CommandResult:
    tenant_adapter = runtime.domain.tenants
    try:
        resolved_tenant = resolve_tenant(tenant, adapter=tenant_adapter)
    except ApiResultError as error:
        raise _translate_tenant_api_error(
            error,
            ds_version=runtime.profile.ds_version,
            operation="delete",
            tenant_code=tenant,
        ) from error
    try:
        deleted = tenant_adapter.delete(tenant_id=resolved_tenant.id)
    except ApiResultError as error:
        raise _translate_tenant_api_error(
            error,
            ds_version=runtime.profile.ds_version,
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


def _resolve_tenant_queue(
    queue: str,
    *,
    runtime: TenantServiceRuntime,
    operation: str,
) -> ResolvedQueue:
    try:
        return resolve_queue(queue, adapter=runtime.domain.queues)
    except ApiResultError as error:
        raise _translate_tenant_api_error(
            error,
            ds_version=runtime.profile.ds_version,
            operation=operation,
            queue_selector=queue,
        ) from error


def _translate_tenant_api_error(
    error: ApiResultError,
    *,
    ds_version: str,
    operation: str,
    tenant_id: int | None = None,
    tenant_code: str | None = None,
    queue_id: int | None = None,
    queue_selector: str | None = None,
) -> Exception:
    details = _tenant_error_details(
        operation=operation,
        tenant_id=tenant_id,
        tenant_code=tenant_code,
        queue_id=queue_id,
        queue_selector=queue_selector,
    )

    if error.result_code == TENANT_NOT_EXIST:
        identifier = tenant_id if tenant_id is not None else tenant_code
        message = f"Tenant {identifier!r} was not found"
        return NotFoundError(message, details=details)
    if error.result_code == QUEUE_NOT_EXIST:
        identifier = queue_id if queue_id is not None else queue_selector
        message = f"Queue {identifier!r} was not found"
        return NotFoundError(message, details=details)
    if (
        ds_version == "1.3.9"
        and operation == "create"
        and error.result_code == REQUEST_PARAMS_NOT_VALID_ERROR
    ):
        message = "Tenant create conflicted with an existing tenant code"
        return ConflictError(message, details=details)
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
    if ds_version == "1.3.9" and error.result_code == VERIFY_TENANT_CODE_ERROR:
        message = "Tenant code was rejected by DolphinScheduler 1.3.9"
        return UserInputError(
            message,
            details=details,
            suggestion=(
                "Use a non-empty operating-system tenant code containing only "
                "characters accepted by the DolphinScheduler 1.3.9 server."
            ),
        )
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


def _tenant_error_details(
    *,
    operation: str,
    tenant_id: int | None,
    tenant_code: str | None,
    queue_id: int | None,
    queue_selector: str | None,
) -> dict[str, str | int]:
    details: dict[str, str | int] = {"operation": operation}
    optional_details: tuple[tuple[str, str | int | None], ...] = (
        ("id", tenant_id),
        ("tenantCode", tenant_code),
        ("queueId", queue_id),
        ("queue", queue_selector),
    )
    details.update((key, value) for key, value in optional_details if value is not None)
    return details
