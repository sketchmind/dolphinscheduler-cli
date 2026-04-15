from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias, TypedDict

from dsctl.cli_surface import DATASOURCE_RESOURCE, NAMESPACE_RESOURCE, USER_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConflictError,
    NotFoundError,
    PermissionDeniedError,
    ResolutionError,
    UserInputError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._serialization import (
    UserListItemData,
    serialize_user,
    serialize_user_list_item,
)
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
from dsctl.services.resolver import (
    ResolvedDataSourceData,
    ResolvedNamespaceData,
    ResolvedProjectData,
    ResolvedUserData,
)
from dsctl.services.resolver import datasource as resolve_datasource
from dsctl.services.resolver import namespace as resolve_namespace
from dsctl.services.resolver import project as resolve_project
from dsctl.services.resolver import tenant as resolve_tenant
from dsctl.services.resolver import user as resolve_user
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from dsctl.upstream.protocol import (
        DataSourceOperations,
        DataSourceRecord,
        NamespaceOperations,
        NamespaceRecord,
        UserRecord,
    )


UserPageData: TypeAlias = PageData[UserListItemData]

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
USER_UPDATE_DIFFERENT_VALUES_SUGGESTION = (
    "Pass a different --user-name, --password, --email, --tenant, --state, "
    "--phone, --clear-phone, --queue, --clear-queue, or --time-zone value."
)


class DeleteUserData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    user: ResolvedUserData


class GrantUserProjectData(TypedDict):
    """CLI project grant confirmation payload."""

    granted: bool
    permission: str
    user: ResolvedUserData
    project: ResolvedProjectData


class RevokeUserProjectData(TypedDict):
    """CLI project revoke confirmation payload."""

    revoked: bool
    user: ResolvedUserData
    project: ResolvedProjectData


class GrantUserNamespacesData(TypedDict):
    """CLI namespace grant confirmation payload."""

    granted: bool
    user: ResolvedUserData
    requested_namespaces: list[ResolvedNamespaceData]
    namespaces: list[ResolvedNamespaceData]


class RevokeUserNamespacesData(TypedDict):
    """CLI namespace revoke confirmation payload."""

    revoked: bool
    user: ResolvedUserData
    requested_namespaces: list[ResolvedNamespaceData]
    namespaces: list[ResolvedNamespaceData]


class GrantUserDatasourcesData(TypedDict):
    """CLI datasource grant confirmation payload."""

    granted: bool
    user: ResolvedUserData
    requested_datasources: list[ResolvedDataSourceData]
    datasources: list[ResolvedDataSourceData]


class RevokeUserDatasourcesData(TypedDict):
    """CLI datasource revoke confirmation payload."""

    revoked: bool
    user: ResolvedUserData
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

    return run_with_service_runtime(
        env_file,
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
    return run_with_service_runtime(
        env_file,
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

    return run_with_service_runtime(
        env_file,
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

    return run_with_service_runtime(
        env_file,
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

    return run_with_service_runtime(
        env_file,
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
    return run_with_service_runtime(
        env_file,
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
    return run_with_service_runtime(
        env_file,
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
    return run_with_service_runtime(
        env_file,
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
    return run_with_service_runtime(
        env_file,
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
    return run_with_service_runtime(
        env_file,
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
    return run_with_service_runtime(
        env_file,
        _revoke_user_namespaces_result,
        user=user,
        namespaces=normalized_namespaces,
    )


def _list_users_result(
    runtime: ServiceRuntime,
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.upstream.users
    data = requested_page_data(
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
        max_pages=MAX_AUTO_EXHAUST_PAGES,
        translate_error=lambda error: _translate_user_api_error(
            error,
            operation="list",
        ),
    )

    return CommandResult(
        data=require_json_object(data, label="user list data"),
        resolved={
            "search": search,
            "page_no": page_no,
            "page_size": page_size,
            "all": all_pages,
        },
    )


def _get_user_result(
    runtime: ServiceRuntime,
    *,
    user: str,
) -> CommandResult:
    adapter = runtime.upstream.users
    resolved_user = resolve_user(user, adapter=adapter)
    fetched_user = adapter.get(user_id=resolved_user.id)
    return CommandResult(
        data=require_json_object(
            serialize_user(fetched_user),
            label="user data",
        ),
        resolved={
            "user": require_json_object(
                resolved_user.to_data(),
                label="resolved user",
            )
        },
    )


def _create_user_result(
    runtime: ServiceRuntime,
    *,
    user_name: str,
    password: str,
    email: str,
    tenant: str,
    state: int,
    phone: str | None,
    queue: str | None,
) -> CommandResult:
    user_adapter = runtime.upstream.users
    resolved_tenant = resolve_tenant(tenant, adapter=runtime.upstream.tenants)
    try:
        created_user = user_adapter.create(
            user_name=user_name,
            password=password,
            email=email,
            tenant_id=resolved_tenant.id,
            phone=phone,
            queue=queue,
            state=state,
        )
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="create",
            user_name=user_name,
            tenant_id=resolved_tenant.id,
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
    runtime: ServiceRuntime,
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
    user_adapter = runtime.upstream.users
    resolved_user = resolve_user(user, adapter=user_adapter)
    current_user = user_adapter.get(user_id=resolved_user.id)

    next_user_name = current_user.userName if user_name is None else user_name
    next_email = current_user.email if email is None else email
    next_tenant_id = current_user.tenantId
    if tenant is not None:
        next_tenant_id = resolve_tenant(tenant, adapter=runtime.upstream.tenants).id

    next_phone = current_user.phone if isinstance(phone, _UnsetValue) else phone
    current_queue = _stored_queue(current_user)
    next_queue = (
        current_queue
        if isinstance(queue, _UnsetValue)
        else ""
        if queue is None
        else queue
    )
    next_state = current_user.state if state is None else state
    next_time_zone = current_user.timeZone if time_zone is None else time_zone
    next_password = "" if password is None else password

    if next_user_name is None or next_email is None:
        message = "User payload was missing required fields"
        raise ApiTransportError(
            message,
            details={"resource": USER_RESOURCE, "id": resolved_user.id},
        )

    if (
        password is None
        and next_user_name == current_user.userName
        and next_email == current_user.email
        and next_tenant_id == current_user.tenantId
        and next_phone == current_user.phone
        and next_queue == current_queue
        and next_state == current_user.state
        and next_time_zone == current_user.timeZone
    ):
        message = "User update requires at least one field change"
        raise UserInputError(
            message,
            suggestion=USER_UPDATE_DIFFERENT_VALUES_SUGGESTION,
        )

    try:
        updated_user = user_adapter.update(
            user_id=resolved_user.id,
            user_name=next_user_name,
            password=next_password,
            email=next_email,
            tenant_id=next_tenant_id,
            phone=next_phone,
            queue=next_queue,
            state=next_state,
            time_zone=next_time_zone,
        )
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="update",
            user_id=resolved_user.id,
            user_name=next_user_name,
            tenant_id=next_tenant_id,
        ) from error

    return CommandResult(
        data=require_json_object(
            serialize_user(updated_user),
            label="user data",
        ),
        resolved={
            "user": require_json_object(
                resolved_user.to_data(),
                label="resolved user",
            )
        },
    )


def _delete_user_result(
    runtime: ServiceRuntime,
    *,
    user: str,
) -> CommandResult:
    user_adapter = runtime.upstream.users
    resolved_user = resolve_user(user, adapter=user_adapter)
    try:
        deleted = user_adapter.delete(user_id=resolved_user.id)
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="delete",
            user_id=resolved_user.id,
            user_name=resolved_user.user_name,
            tenant_id=resolved_user.tenant_id,
        ) from error

    data: DeleteUserData = {
        "deleted": deleted,
        "user": resolved_user.to_data(),
    }
    return CommandResult(
        data=require_json_object(data, label="user delete data"),
        resolved={
            "user": require_json_object(
                resolved_user.to_data(),
                label="resolved user",
            )
        },
    )


def _grant_user_project_result(
    runtime: ServiceRuntime,
    *,
    user: str,
    project: str,
) -> CommandResult:
    user_adapter = runtime.upstream.users
    resolved_user = resolve_user(user, adapter=user_adapter)
    resolved_project = resolve_project(project, adapter=runtime.upstream.projects)
    try:
        granted = user_adapter.grant_project_by_code(
            user_id=resolved_user.id,
            project_code=resolved_project.code,
        )
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="grant_project",
            user_id=resolved_user.id,
            user_name=resolved_user.user_name,
            tenant_id=resolved_user.tenant_id,
            project_code=resolved_project.code,
            project_name=resolved_project.name,
        ) from error

    data: GrantUserProjectData = {
        "granted": granted,
        "permission": "write",
        "user": resolved_user.to_data(),
        "project": resolved_project.to_data(),
    }
    return CommandResult(
        data=require_json_object(data, label="user project grant data"),
        resolved={
            "user": require_json_object(
                resolved_user.to_data(),
                label="resolved user",
            ),
            "project": require_json_object(
                resolved_project.to_data(),
                label="resolved project",
            ),
        },
    )


def _revoke_user_project_result(
    runtime: ServiceRuntime,
    *,
    user: str,
    project: str,
) -> CommandResult:
    user_adapter = runtime.upstream.users
    resolved_user = resolve_user(user, adapter=user_adapter)
    resolved_project = resolve_project(project, adapter=runtime.upstream.projects)
    try:
        revoked = user_adapter.revoke_project(
            user_id=resolved_user.id,
            project_code=resolved_project.code,
        )
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="revoke_project",
            user_id=resolved_user.id,
            user_name=resolved_user.user_name,
            tenant_id=resolved_user.tenant_id,
            project_code=resolved_project.code,
            project_name=resolved_project.name,
        ) from error

    data: RevokeUserProjectData = {
        "revoked": revoked,
        "user": resolved_user.to_data(),
        "project": resolved_project.to_data(),
    }
    return CommandResult(
        data=require_json_object(data, label="user project revoke data"),
        resolved={
            "user": require_json_object(
                resolved_user.to_data(),
                label="resolved user",
            ),
            "project": require_json_object(
                resolved_project.to_data(),
                label="resolved project",
            ),
        },
    )


def _grant_user_datasources_result(
    runtime: ServiceRuntime,
    *,
    user: str,
    datasources: Sequence[str],
) -> CommandResult:
    user_adapter = runtime.upstream.users
    datasource_adapter = runtime.upstream.datasources
    resolved_user = resolve_user(user, adapter=user_adapter)
    requested_datasources = _resolve_requested_datasources(
        datasources,
        adapter=datasource_adapter,
    )
    current_datasources = _authorized_datasources(
        datasource_adapter,
        user_id=resolved_user.id,
    )

    final_by_id = {item["id"]: item for item in current_datasources}
    for datasource in requested_datasources:
        final_by_id[datasource["id"]] = datasource

    try:
        granted = user_adapter.grant_datasources(
            user_id=resolved_user.id,
            datasource_ids=sorted(final_by_id),
        )
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="grant_datasources",
            user_id=resolved_user.id,
            user_name=resolved_user.user_name,
            tenant_id=resolved_user.tenant_id,
        ) from error

    data: GrantUserDatasourcesData = {
        "granted": granted,
        "user": resolved_user.to_data(),
        "requested_datasources": requested_datasources,
        "datasources": _sorted_datasource_data(final_by_id.values()),
    }
    return CommandResult(
        data=require_json_object(data, label="user datasource grant data"),
        resolved={
            "user": require_json_object(
                resolved_user.to_data(),
                label="resolved user",
            ),
            "datasources": _json_datasource_list(requested_datasources),
        },
    )


def _revoke_user_datasources_result(
    runtime: ServiceRuntime,
    *,
    user: str,
    datasources: Sequence[str],
) -> CommandResult:
    user_adapter = runtime.upstream.users
    datasource_adapter = runtime.upstream.datasources
    resolved_user = resolve_user(user, adapter=user_adapter)
    requested_datasources = _resolve_requested_datasources(
        datasources,
        adapter=datasource_adapter,
    )
    current_datasources = _authorized_datasources(
        datasource_adapter,
        user_id=resolved_user.id,
    )

    requested_ids = {datasource["id"] for datasource in requested_datasources}
    final_by_id = {
        item["id"]: item
        for item in current_datasources
        if item["id"] not in requested_ids
    }

    try:
        revoked = user_adapter.grant_datasources(
            user_id=resolved_user.id,
            datasource_ids=sorted(final_by_id),
        )
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="revoke_datasources",
            user_id=resolved_user.id,
            user_name=resolved_user.user_name,
            tenant_id=resolved_user.tenant_id,
        ) from error

    data: RevokeUserDatasourcesData = {
        "revoked": revoked,
        "user": resolved_user.to_data(),
        "requested_datasources": requested_datasources,
        "datasources": _sorted_datasource_data(final_by_id.values()),
    }
    return CommandResult(
        data=require_json_object(data, label="user datasource revoke data"),
        resolved={
            "user": require_json_object(
                resolved_user.to_data(),
                label="resolved user",
            ),
            "datasources": _json_datasource_list(requested_datasources),
        },
    )


def _grant_user_namespaces_result(
    runtime: ServiceRuntime,
    *,
    user: str,
    namespaces: Sequence[str],
) -> CommandResult:
    user_adapter = runtime.upstream.users
    namespace_adapter = runtime.upstream.namespaces
    resolved_user = resolve_user(user, adapter=user_adapter)
    requested_namespaces = _resolve_requested_namespaces(
        namespaces,
        adapter=namespace_adapter,
    )
    current_namespaces = _authorized_namespaces(
        namespace_adapter,
        user_id=resolved_user.id,
    )

    final_by_id = {item["id"]: item for item in current_namespaces}
    for namespace in requested_namespaces:
        final_by_id[namespace["id"]] = namespace

    try:
        granted = user_adapter.grant_namespaces(
            user_id=resolved_user.id,
            namespace_ids=sorted(final_by_id),
        )
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="grant_namespaces",
            user_id=resolved_user.id,
            user_name=resolved_user.user_name,
            tenant_id=resolved_user.tenant_id,
        ) from error

    data: GrantUserNamespacesData = {
        "granted": granted,
        "user": resolved_user.to_data(),
        "requested_namespaces": requested_namespaces,
        "namespaces": _sorted_namespace_data(final_by_id.values()),
    }
    return CommandResult(
        data=require_json_object(data, label="user namespace grant data"),
        resolved={
            "user": require_json_object(
                resolved_user.to_data(),
                label="resolved user",
            ),
            "namespaces": _json_namespace_list(requested_namespaces),
        },
    )


def _revoke_user_namespaces_result(
    runtime: ServiceRuntime,
    *,
    user: str,
    namespaces: Sequence[str],
) -> CommandResult:
    user_adapter = runtime.upstream.users
    namespace_adapter = runtime.upstream.namespaces
    resolved_user = resolve_user(user, adapter=user_adapter)
    requested_namespaces = _resolve_requested_namespaces(
        namespaces,
        adapter=namespace_adapter,
    )
    current_namespaces = _authorized_namespaces(
        namespace_adapter,
        user_id=resolved_user.id,
    )

    requested_ids = {namespace["id"] for namespace in requested_namespaces}
    final_by_id = {
        item["id"]: item
        for item in current_namespaces
        if item["id"] not in requested_ids
    }

    try:
        revoked = user_adapter.grant_namespaces(
            user_id=resolved_user.id,
            namespace_ids=sorted(final_by_id),
        )
    except ApiResultError as error:
        raise _translate_user_api_error(
            error,
            operation="revoke_namespaces",
            user_id=resolved_user.id,
            user_name=resolved_user.user_name,
            tenant_id=resolved_user.tenant_id,
        ) from error

    data: RevokeUserNamespacesData = {
        "revoked": revoked,
        "user": resolved_user.to_data(),
        "requested_namespaces": requested_namespaces,
        "namespaces": _sorted_namespace_data(final_by_id.values()),
    }
    return CommandResult(
        data=require_json_object(data, label="user namespace revoke data"),
        resolved={
            "user": require_json_object(
                resolved_user.to_data(),
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


def _resolved_datasource_data_from_record(
    datasource: DataSourceRecord,
) -> ResolvedDataSourceData:
    if datasource.id is None or datasource.name is None:
        message = "Datasource payload was missing required identity fields"
        raise ResolutionError(
            message,
            details={"resource": DATASOURCE_RESOURCE},
        )
    return {
        "id": datasource.id,
        "name": datasource.name,
        "note": datasource.note,
        "type": None if datasource.type is None else datasource.type.value,
    }


def _resolved_namespace_data_from_record(
    namespace: NamespaceRecord,
) -> ResolvedNamespaceData:
    if namespace.id is None or namespace.namespace is None:
        message = "Namespace payload was missing required identity fields"
        raise ResolutionError(
            message,
            details={"resource": NAMESPACE_RESOURCE},
        )
    return {
        "id": namespace.id,
        "namespace": namespace.namespace,
        "clusterCode": namespace.clusterCode,
        "clusterName": namespace.clusterName,
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


def _resolve_requested_datasources(
    identifiers: Sequence[str],
    *,
    adapter: DataSourceOperations,
) -> list[ResolvedDataSourceData]:
    resolved_by_id: dict[int, ResolvedDataSourceData] = {}
    for identifier in identifiers:
        resolved = resolve_datasource(identifier, adapter=adapter).to_data()
        resolved_by_id.setdefault(resolved["id"], resolved)
    return _sorted_datasource_data(resolved_by_id.values())


def _authorized_datasources(
    adapter: DataSourceOperations,
    *,
    user_id: int,
) -> list[ResolvedDataSourceData]:
    return _sorted_datasource_data(
        _resolved_datasource_data_from_record(record)
        for record in adapter.authorized_for_user(user_id=user_id)
    )


def _resolve_requested_namespaces(
    identifiers: Sequence[str],
    *,
    adapter: NamespaceOperations,
) -> list[ResolvedNamespaceData]:
    resolved_by_id: dict[int, ResolvedNamespaceData] = {}
    for identifier in identifiers:
        resolved = resolve_namespace(identifier, adapter=adapter).to_data()
        resolved_by_id.setdefault(resolved["id"], resolved)
    return _sorted_namespace_data(resolved_by_id.values())


def _authorized_namespaces(
    adapter: NamespaceOperations,
    *,
    user_id: int,
) -> list[ResolvedNamespaceData]:
    return _sorted_namespace_data(
        _resolved_namespace_data_from_record(record)
        for record in adapter.authorized_for_user(user_id=user_id)
    )


def _sorted_datasource_data(
    datasources: Iterable[ResolvedDataSourceData],
) -> list[ResolvedDataSourceData]:
    return sorted(datasources, key=lambda datasource: datasource["id"])


def _json_datasource_list(
    datasources: Iterable[ResolvedDataSourceData],
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


def _sorted_namespace_data(
    namespaces: Iterable[ResolvedNamespaceData],
) -> list[ResolvedNamespaceData]:
    return sorted(namespaces, key=lambda namespace: namespace["id"])


def _json_namespace_list(
    namespaces: Iterable[ResolvedNamespaceData],
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


def _stored_queue(user: UserRecord) -> str:
    stored_queue = user.storedQueue
    return "" if stored_queue is None else stored_queue


def _optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None
