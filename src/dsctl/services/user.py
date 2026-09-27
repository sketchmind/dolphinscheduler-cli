from __future__ import annotations

from typing import TYPE_CHECKING, Literal, TypeAlias, TypedDict, cast

from dsctl.cli_surface import USER_RESOURCE
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
from dsctl.upstream.serialization import (
    serialize_user,
    serialize_user_list_item,
)
from dsctl.upstream.users import (
    USER_DOMAIN,
    PermissionDataSource,
    PermissionNamespace,
    UserDomain,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.output import JsonObject
    from dsctl.upstream.protocol import UserRecord
    from dsctl.upstream.resolver import (
        ResolvedDataSourceData,
        ResolvedNamespaceData,
        ResolvedUserData,
    )
    from dsctl.upstream.users import UserIdentityData


UserServiceRuntime: TypeAlias = BoundDomainServiceRuntime[UserDomain]

REQUEST_PARAMS_NOT_VALID_ERROR = 10001
USER_NAME_EXIST = 10003
USER_NOT_EXIST = 10010
TENANT_NOT_EXIST = 10017
PROJECT_NOT_FOUND = 10018
CREATE_USER_ERROR = 10090
UPDATE_USER_ERROR = 10092
DELETE_USER_BY_ID_ERROR = 10093
TRANSFORM_PROJECT_OWNERSHIP = 10179
USER_NO_OPERATION_PERM = 30001
NO_CURRENT_OPERATING_PERMISSION = 1400001
NOT_ALLOW_TO_DISABLE_OWN_ACCOUNT = 130020
USER_PASSWORD_LENGTH_ERROR = 1300017

USER_UPDATE_FIELDS_SUGGESTION = (
    "Pass at least one update flag such as --user-name, --password, --email, "
    "--tenant, --state, --phone, --clear-phone, --queue, --clear-queue, or "
    "--time-zone."
)


class DeleteUserData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    user: ResolvedUserData | UserIdentityData


class GrantUserProjectData(TypedDict):
    """CLI project grant confirmation payload."""

    granted: bool
    permission: str
    verification: Literal["membership_only"]
    user: ResolvedUserData | UserIdentityData
    project: JsonObject


class RevokeUserProjectData(TypedDict):
    """CLI project revoke confirmation payload."""

    revoked: bool
    user: ResolvedUserData | UserIdentityData
    project: JsonObject


class GrantUserNamespacesData(TypedDict):
    """CLI namespace grant confirmation payload."""

    granted: bool
    user: ResolvedUserData | UserIdentityData
    requested_namespaces: list[ResolvedNamespaceData]
    namespaces: list[ResolvedNamespaceData]


class RevokeUserNamespacesData(TypedDict):
    """CLI namespace revoke confirmation payload."""

    revoked: bool
    user: ResolvedUserData | UserIdentityData
    requested_namespaces: list[ResolvedNamespaceData]
    namespaces: list[ResolvedNamespaceData]


class GrantUserDatasourcesData(TypedDict):
    """CLI datasource grant confirmation payload."""

    granted: bool
    user: ResolvedUserData | UserIdentityData
    requested_datasources: list[ResolvedDataSourceData]
    datasources: list[ResolvedDataSourceData]


class RevokeUserDatasourcesData(TypedDict):
    """CLI datasource revoke confirmation payload."""

    revoked: bool
    user: ResolvedUserData | UserIdentityData
    requested_datasources: list[ResolvedDataSourceData]
    datasources: list[ResolvedDataSourceData]


class _UnsetValue:
    """Sentinel for update fields that should keep their current value."""


UNSET = _UnsetValue()
PhoneUpdate = str | None | _UnsetValue
QueueUpdate = str | None | _UnsetValue


def list_users_result(
    *,
    env_file: str | None = None,
    search: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
) -> CommandResult:
    """List users with explicit paging or auto-exhaust support."""
    normalized_search = _optional_text(search)
    require_positive_int(page_no, label="page_no")
    require_positive_int(page_size, label="page_size")

    return run_with_bound_domain_service_runtime(
        env_file,
        USER_DOMAIN,
        _list_users_result,
        search=normalized_search,
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
    )


def get_user_result(
    user: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Resolve and fetch one user payload."""
    return run_with_bound_domain_service_runtime(
        env_file,
        USER_DOMAIN,
        _get_user_result,
        user=user,
    )


def create_user_result(
    *,
    user_name: str,
    password: str,
    email: str,
    tenant: str,
    state: int,
    phone: str | None = None,
    queue: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create one user from validated CLI input."""
    normalized_user_name = require_non_empty_text(user_name, label="user name")
    normalized_password = require_non_empty_text(password, label="password")
    normalized_email = require_non_empty_text(email, label="email")
    normalized_tenant = require_non_empty_text(tenant, label="tenant")
    normalized_state = _require_user_state(state)
    normalized_phone = (
        None if phone is None else require_non_empty_text(phone, label="phone")
    )
    normalized_queue = (
        None if queue is None else require_non_empty_text(queue, label="queue")
    )

    return run_with_bound_domain_service_runtime(
        env_file,
        USER_DOMAIN,
        _create_user_result,
        user_name=normalized_user_name,
        password=normalized_password,
        email=normalized_email,
        tenant=normalized_tenant,
        state=normalized_state,
        phone=normalized_phone,
        queue=normalized_queue,
    )


def update_user_result(
    user: str,
    *,
    user_name: str | None = None,
    password: str | None = None,
    email: str | None = None,
    tenant: str | None = None,
    state: int | None = None,
    phone: PhoneUpdate = UNSET,
    queue: QueueUpdate = UNSET,
    time_zone: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Update one user while preserving omitted fields."""
    if (
        user_name is None
        and password is None
        and email is None
        and tenant is None
        and state is None
        and isinstance(phone, _UnsetValue)
        and isinstance(queue, _UnsetValue)
        and time_zone is None
    ):
        message = "User update requires at least one field change"
        raise UserInputError(message, suggestion=USER_UPDATE_FIELDS_SUGGESTION)

    normalized_user_name = (
        require_non_empty_text(user_name, label="user name")
        if user_name is not None
        else None
    )
    normalized_password = (
        require_non_empty_text(password, label="password")
        if password is not None
        else None
    )
    normalized_email = (
        require_non_empty_text(email, label="email") if email is not None else None
    )
    normalized_tenant = (
        require_non_empty_text(tenant, label="tenant") if tenant is not None else None
    )
    normalized_state = _require_user_state(state) if state is not None else None
    normalized_phone = (
        UNSET
        if isinstance(phone, _UnsetValue)
        else None
        if phone is None
        else require_non_empty_text(phone, label="phone")
    )
    normalized_queue = (
        UNSET
        if isinstance(queue, _UnsetValue)
        else None
        if queue is None
        else require_non_empty_text(queue, label="queue")
    )
    normalized_time_zone = (
        None
        if time_zone is None
        else require_non_empty_text(time_zone, label="time zone")
    )

    return run_with_bound_domain_service_runtime(
        env_file,
        USER_DOMAIN,
        _update_user_result,
        user=user,
        user_name=normalized_user_name,
        password=normalized_password,
        email=normalized_email,
        tenant=normalized_tenant,
        state=normalized_state,
        phone=normalized_phone,
        queue=normalized_queue,
        time_zone=normalized_time_zone,
    )


def delete_user_result(
    user: str,
    *,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one user after explicit confirmation."""
    require_delete_force(force=force, resource_label="User")

    return run_with_bound_domain_service_runtime(
        env_file,
        USER_DOMAIN,
        _delete_user_result,
        user=user,
    )


def grant_user_project_result(
    user: str,
    project: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Grant one project to one resolved user."""
    return run_with_bound_domain_service_runtime(
        env_file,
        USER_DOMAIN,
        _grant_user_project_result,
        user=user,
        project=project,
    )


def revoke_user_project_result(
    user: str,
    project: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Revoke one project from one resolved user."""
    return run_with_bound_domain_service_runtime(
        env_file,
        USER_DOMAIN,
        _revoke_user_project_result,
        user=user,
        project=project,
    )


def grant_user_datasources_result(
    user: str,
    datasources: Sequence[str],
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Grant one or more datasources to one resolved user."""
    normalized_datasources = _required_identifiers(
        datasources,
        label="datasource",
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        USER_DOMAIN,
        _grant_user_datasources_result,
        user=user,
        datasources=normalized_datasources,
    )


def revoke_user_datasources_result(
    user: str,
    datasources: Sequence[str],
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Revoke one or more datasources from one resolved user."""
    normalized_datasources = _required_identifiers(
        datasources,
        label="datasource",
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        USER_DOMAIN,
        _revoke_user_datasources_result,
        user=user,
        datasources=normalized_datasources,
    )


def grant_user_namespaces_result(
    user: str,
    namespaces: Sequence[str],
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Grant one or more namespaces to one resolved user."""
    normalized_namespaces = _required_identifiers(
        namespaces,
        label="namespace",
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        USER_DOMAIN,
        _grant_user_namespaces_result,
        user=user,
        namespaces=normalized_namespaces,
    )


def revoke_user_namespaces_result(
    user: str,
    namespaces: Sequence[str],
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Revoke one or more namespaces from one resolved user."""
    normalized_namespaces = _required_identifiers(
        namespaces,
        label="namespace",
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        USER_DOMAIN,
        _revoke_user_namespaces_result,
        user=user,
        namespaces=normalized_namespaces,
    )


def _list_users_result(
    runtime: UserServiceRuntime,
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.domain.users
    return paged_command_result(
        lambda current_page_no, current_page_size: adapter.list(
            page_no=current_page_no,
            page_size=current_page_size,
            search=search,
        ),
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=serialize_user_list_item,
        resource=USER_RESOURCE,
        resolved={"search": search},
        translate_error=lambda error: _translate_user_api_error(
            error,
            operation="list",
        ),
    )


def _get_user_result(
    runtime: UserServiceRuntime,
    *,
    user: str,
) -> CommandResult:
    selected = runtime.domain.get(user)
    return CommandResult(
        data=require_json_object(
            serialize_user(selected.record),
            label="user data",
        ),
        resolved={
            "user": require_json_object(
                selected.resolved.to_data(),
                label="resolved user",
            )
        },
    )


def _create_user_result(
    runtime: UserServiceRuntime,
    *,
    user_name: str,
    password: str,
    email: str,
    tenant: str,
    state: int,
    phone: str | None,
    queue: str | None,
) -> CommandResult:
    try:
        created_user = runtime.domain.create(
            user_name=user_name,
            password=password,
            email=email,
            tenant=tenant,
            phone=phone,
            queue=queue,
            state=state,
        )
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="create",
            user_name=user_name,
        ) from error

    return CommandResult(
        data=require_json_object(
            serialize_user(created_user),
            label="user data",
        ),
        resolved={
            "user": require_json_object(
                _resolved_user_data_from_record(created_user),
                label="resolved user",
            )
        },
    )


def _update_user_result(
    runtime: UserServiceRuntime,
    *,
    user: str,
    user_name: str | None,
    password: str | None,
    email: str | None,
    tenant: str | None,
    state: int | None,
    phone: PhoneUpdate,
    queue: QueueUpdate,
    time_zone: str | None,
) -> CommandResult:
    try:
        selected = runtime.domain.update(
            user,
            user_name=user_name,
            password=password,
            email=email,
            tenant=tenant,
            state=state,
            phone=None if isinstance(phone, _UnsetValue) else phone,
            preserve_phone=isinstance(phone, _UnsetValue),
            queue=None if isinstance(queue, _UnsetValue) else queue,
            preserve_queue=isinstance(queue, _UnsetValue),
            time_zone=time_zone,
        )
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="update",
            user_name=user,
        ) from error

    return CommandResult(
        data=require_json_object(
            serialize_user(selected.record),
            label="user data",
        ),
        resolved={
            "user": require_json_object(
                selected.resolved.to_data(),
                label="resolved user",
            )
        },
    )


def _delete_user_result(
    runtime: UserServiceRuntime,
    *,
    user: str,
) -> CommandResult:
    try:
        deletion = runtime.domain.delete(user)
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="delete",
            user_name=user,
        ) from error

    data: DeleteUserData = {
        "deleted": deletion.deleted,
        "user": deletion.resolved.to_data(),
    }
    return CommandResult(
        data=require_json_object(data, label="user delete data"),
        resolved={
            "user": require_json_object(
                deletion.resolved.to_data(),
                label="resolved user",
            )
        },
    )


def _grant_user_project_result(
    runtime: UserServiceRuntime,
    *,
    user: str,
    project: str,
) -> CommandResult:
    try:
        change = runtime.domain.grant_project(user, project)
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="grant_project",
            user_name=user,
            project_name=project,
        ) from error

    data: GrantUserProjectData = {
        "granted": True,
        "permission": "write",
        "verification": "membership_only",
        "user": change.user.to_data(),
        "project": change.project.to_data(),
    }
    return CommandResult(
        data=require_json_object(data, label="user project grant data"),
        resolved={
            "user": require_json_object(
                change.user.to_data(),
                label="resolved user",
            ),
            "project": require_json_object(
                change.project.to_data(),
                label="resolved project",
            ),
        },
    )


def _revoke_user_project_result(
    runtime: UserServiceRuntime,
    *,
    user: str,
    project: str,
) -> CommandResult:
    try:
        change = runtime.domain.revoke_project(user, project)
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="revoke_project",
            user_name=user,
            project_name=project,
        ) from error

    data: RevokeUserProjectData = {
        "revoked": True,
        "user": change.user.to_data(),
        "project": change.project.to_data(),
    }
    return CommandResult(
        data=require_json_object(data, label="user project revoke data"),
        resolved={
            "user": require_json_object(
                change.user.to_data(),
                label="resolved user",
            ),
            "project": require_json_object(
                change.project.to_data(),
                label="resolved project",
            ),
        },
    )


def _grant_user_datasources_result(
    runtime: UserServiceRuntime,
    *,
    user: str,
    datasources: Sequence[str],
) -> CommandResult:
    try:
        change = runtime.domain.change_datasources(
            user,
            datasources,
            grant=True,
        )
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="grant_datasources",
            user_name=user,
        ) from error
    requested_datasources = _datasource_data(change.requested)
    final_datasources = _datasource_data(change.final)

    data: GrantUserDatasourcesData = {
        "granted": True,
        "user": change.user.to_data(),
        "requested_datasources": requested_datasources,
        "datasources": final_datasources,
    }
    return CommandResult(
        data=require_json_object(data, label="user datasource grant data"),
        resolved={
            "user": require_json_object(
                change.user.to_data(),
                label="resolved user",
            ),
            "datasources": _json_datasource_list(requested_datasources),
        },
    )


def _revoke_user_datasources_result(
    runtime: UserServiceRuntime,
    *,
    user: str,
    datasources: Sequence[str],
) -> CommandResult:
    try:
        change = runtime.domain.change_datasources(
            user,
            datasources,
            grant=False,
        )
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="revoke_datasources",
            user_name=user,
        ) from error
    requested_datasources = _datasource_data(change.requested)
    final_datasources = _datasource_data(change.final)

    data: RevokeUserDatasourcesData = {
        "revoked": True,
        "user": change.user.to_data(),
        "requested_datasources": requested_datasources,
        "datasources": final_datasources,
    }
    return CommandResult(
        data=require_json_object(data, label="user datasource revoke data"),
        resolved={
            "user": require_json_object(
                change.user.to_data(),
                label="resolved user",
            ),
            "datasources": _json_datasource_list(requested_datasources),
        },
    )


def _grant_user_namespaces_result(
    runtime: UserServiceRuntime,
    *,
    user: str,
    namespaces: Sequence[str],
) -> CommandResult:
    try:
        change = runtime.domain.change_namespaces(
            user,
            namespaces,
            grant=True,
        )
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="grant_namespaces",
            user_name=user,
        ) from error
    requested_namespaces = _namespace_data(change.requested)
    final_namespaces = _namespace_data(change.final)

    data: GrantUserNamespacesData = {
        "granted": True,
        "user": change.user.to_data(),
        "requested_namespaces": requested_namespaces,
        "namespaces": final_namespaces,
    }
    return CommandResult(
        data=require_json_object(data, label="user namespace grant data"),
        resolved={
            "user": require_json_object(
                change.user.to_data(),
                label="resolved user",
            ),
            "namespaces": _json_namespace_list(requested_namespaces),
        },
    )


def _revoke_user_namespaces_result(
    runtime: UserServiceRuntime,
    *,
    user: str,
    namespaces: Sequence[str],
) -> CommandResult:
    try:
        change = runtime.domain.change_namespaces(
            user,
            namespaces,
            grant=False,
        )
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="revoke_namespaces",
            user_name=user,
        ) from error
    requested_namespaces = _namespace_data(change.requested)
    final_namespaces = _namespace_data(change.final)

    data: RevokeUserNamespacesData = {
        "revoked": True,
        "user": change.user.to_data(),
        "requested_namespaces": requested_namespaces,
        "namespaces": final_namespaces,
    }
    return CommandResult(
        data=require_json_object(data, label="user namespace revoke data"),
        resolved={
            "user": require_json_object(
                change.user.to_data(),
                label="resolved user",
            ),
            "namespaces": _json_namespace_list(requested_namespaces),
        },
    )


def _resolved_user_data_from_record(user: UserRecord) -> ResolvedUserData:
    if user.id is None or user.userName is None:
        message = "User payload was missing required identity fields"
        raise ApiTransportError(
            message,
            details={"resource": USER_RESOURCE},
        )
    return {
        "id": user.id,
        "userName": user.userName,
        "email": user.email,
        "tenantId": user.tenantId,
        "tenantCode": user.tenantCode,
        "state": user.state,
    }


def _required_identifiers(
    identifiers: Sequence[str],
    *,
    label: str,
) -> list[str]:
    normalized = [
        identifier.strip() for identifier in identifiers if identifier.strip()
    ]
    if normalized:
        return normalized
    message = f"At least one {label} is required"
    raise UserInputError(
        message,
        suggestion=f"Pass at least one --{label} value.",
    )


def _datasource_data(
    datasources: Sequence[PermissionDataSource],
) -> list[ResolvedDataSourceData]:
    return sorted(
        [
            cast("ResolvedDataSourceData", datasource.to_data())
            for datasource in datasources
        ],
        key=lambda datasource: datasource["id"],
    )


def _json_datasource_list(
    datasources: Sequence[ResolvedDataSourceData],
) -> list[dict[str, int | str | None]]:
    return [
        {
            "id": datasource["id"],
            "name": datasource["name"],
            "note": datasource["note"],
            "type": datasource["type"],
        }
        for datasource in datasources
    ]


def _namespace_data(
    namespaces: Sequence[PermissionNamespace],
) -> list[ResolvedNamespaceData]:
    return sorted(
        [
            cast("ResolvedNamespaceData", namespace.to_data())
            for namespace in namespaces
        ],
        key=lambda namespace: namespace["id"],
    )


def _json_namespace_list(
    namespaces: Sequence[ResolvedNamespaceData],
) -> list[dict[str, int | str | None]]:
    return [
        {
            "id": namespace["id"],
            "namespace": namespace["namespace"],
            "clusterCode": namespace["clusterCode"],
            "clusterName": namespace["clusterName"],
        }
        for namespace in namespaces
    ]


def _user_error_details(
    *,
    operation: str,
    user_id: int | None,
    user_name: str | None,
    tenant_id: int | None,
    project_code: int | None,
    project_name: str | None,
) -> dict[str, str | int]:
    details: dict[str, str | int] = {"operation": operation}
    if user_id is not None:
        details["id"] = user_id
    if user_name is not None:
        details["userName"] = user_name
    if tenant_id is not None:
        details["tenantId"] = tenant_id
    if project_code is not None:
        details["projectCode"] = project_code
    if project_name is not None:
        details["projectName"] = project_name
    return details


def _user_not_found_error(
    result_code: int | None,
    *,
    details: dict[str, str | int],
    user_id: int | None,
    user_name: str | None,
    tenant_id: int | None,
    project_code: int | None,
    project_name: str | None,
) -> NotFoundError | None:
    if result_code == USER_NOT_EXIST:
        identifier = user_id if user_id is not None else user_name
        message = f"User {identifier!r} was not found"
        return NotFoundError(message, details=details)
    if result_code == TENANT_NOT_EXIST:
        message = f"Tenant {tenant_id!r} was not found"
        return NotFoundError(message, details=details)
    if result_code == PROJECT_NOT_FOUND:
        identifier = project_code if project_code is not None else project_name
        message = f"Project {identifier!r} was not found"
        return NotFoundError(message, details=details)
    return None


def _user_conflict_error(
    result_code: int | None,
    *,
    details: dict[str, str | int],
) -> ConflictError | None:
    if result_code == USER_NAME_EXIST:
        message = "User create/update conflicted with an existing user name"
        return ConflictError(message, details=details)
    if result_code == TRANSFORM_PROJECT_OWNERSHIP:
        message = "User owns projects and cannot be deleted until ownership changes"
        return ConflictError(message, details=details)
    return None


def _translate_user_api_error(
    error: ApiResultError,
    *,
    operation: str,
    user_id: int | None = None,
    user_name: str | None = None,
    tenant_id: int | None = None,
    project_code: int | None = None,
    project_name: str | None = None,
) -> Exception:
    details = _user_error_details(
        operation=operation,
        user_id=user_id,
        user_name=user_name,
        tenant_id=tenant_id,
        project_code=project_code,
        project_name=project_name,
    )

    not_found_error = _user_not_found_error(
        error.result_code,
        details=details,
        user_id=user_id,
        user_name=user_name,
        tenant_id=tenant_id,
        project_code=project_code,
        project_name=project_name,
    )
    if not_found_error is not None:
        return not_found_error

    conflict_error = _user_conflict_error(
        error.result_code,
        details=details,
    )
    if conflict_error is not None:
        return conflict_error

    if error.result_code in (
        USER_NO_OPERATION_PERM,
        NO_CURRENT_OPERATING_PERMISSION,
    ):
        message = f"User {operation} requires additional permissions"
        return PermissionDeniedError(message, details=details)
    if error.result_code in (
        REQUEST_PARAMS_NOT_VALID_ERROR,
        NOT_ALLOW_TO_DISABLE_OWN_ACCOUNT,
        USER_PASSWORD_LENGTH_ERROR,
        CREATE_USER_ERROR,
        UPDATE_USER_ERROR,
        DELETE_USER_BY_ID_ERROR,
    ):
        message = "User input was rejected by the upstream API"
        return UserInputError(
            message,
            details=details,
            suggestion=_user_operation_input_suggestion(operation),
        )
    return error


def _require_user_state(value: int) -> int:
    if value not in (0, 1):
        message = "User state must be 0 or 1"
        raise UserInputError(
            message,
            suggestion="Use --state 1 for enabled or --state 0 for disabled.",
        )
    return value


def _user_operation_input_suggestion(operation: str) -> str:
    if operation == "create":
        return (
            "Verify --user-name, --password, --email, --tenant, --state, "
            "and optional --phone/--queue values, then retry."
        )
    if operation == "update":
        return (
            "Verify the requested user update flags, then retry the same "
            "`dsctl user update ...` command."
        )
    if operation == "delete":
        return "Verify the target user identifier, then retry the delete command."
    if operation in {"grant_project", "revoke_project"}:
        return "Verify the user and project identifiers, then retry."
    if operation in {"grant_datasources", "revoke_datasources"}:
        return "Verify the user and --datasource values, then retry."
    if operation in {"grant_namespaces", "revoke_namespaces"}:
        return "Verify the user and --namespace values, then retry."
    return "Verify the command arguments, then retry."


def _optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None
