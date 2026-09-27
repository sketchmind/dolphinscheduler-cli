from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

from dsctl.cli_surface import ENV_RESOURCE
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
from dsctl.upstream.environments import (
    ENVIRONMENT_DOMAIN,
    EnvironmentDomain,
)
from dsctl.upstream.pagination import DEFAULT_PAGE_SIZE
from dsctl.upstream.resolver import ResolvedEnvironmentData
from dsctl.upstream.resolver import environment as resolve_environment
from dsctl.upstream.serialization import (
    optional_text,
    serialize_environment,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.upstream.protocol import (
        EnvironmentPayloadRecord,
    )


ENVIRONMENT_NAME_EXISTS = 120002
ENVIRONMENT_NAME_IS_NULL = 120003
ENVIRONMENT_CONFIG_IS_NULL = 120004
DELETE_ENVIRONMENT_RELATED_TASK_EXISTS = 120007
QUERY_ENVIRONMENT_BY_CODE_ERROR = 1200009
LEGACY_ENVIRONMENT_WORKER_GROUPS_IS_INVALID = 1200012
LEGACY_UPDATE_ENVIRONMENT_WORKER_GROUP_RELATION_ERROR = 1200013
ENVIRONMENT_WORKER_GROUPS_IS_INVALID = 130015
UPDATE_ENVIRONMENT_WORKER_GROUP_RELATION_ERROR = 130016
USER_NO_OPERATION_PERM = 30001


class DeleteEnvironmentData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    environment: ResolvedEnvironmentData


class _UnsetValue:
    """Sentinel for update fields that should keep their current value."""


UNSET = _UnsetValue()
DescriptionUpdate = str | None | _UnsetValue
WorkerGroupsUpdate = list[str] | _UnsetValue


def list_environments_result(
    *,
    env_file: str | None = None,
    search: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
) -> CommandResult:
    """List environments with explicit paging or auto-exhaust support."""
    normalized_search = optional_text(search)
    require_positive_int(page_no, label="page_no")
    require_positive_int(page_size, label="page_size")

    return run_with_bound_domain_service_runtime(
        env_file,
        ENVIRONMENT_DOMAIN,
        _list_environments_result,
        search=normalized_search,
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
    )


def get_environment_result(
    environment: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Resolve and fetch a single environment."""
    return run_with_bound_domain_service_runtime(
        env_file,
        ENVIRONMENT_DOMAIN,
        _get_environment_result,
        environment=environment,
    )


def create_environment_result(
    *,
    name: str,
    config: str,
    description: str | None = None,
    worker_groups: Sequence[str] | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create one environment from validated CLI input."""
    environment_name = require_non_empty_text(name, label="environment name")
    environment_config = require_non_empty_text(config, label="environment config")
    environment_description = optional_text(description)
    normalized_worker_groups = (
        None if worker_groups is None else _normalize_worker_groups(worker_groups)
    )

    return run_with_bound_domain_service_runtime(
        env_file,
        ENVIRONMENT_DOMAIN,
        _create_environment_result,
        name=environment_name,
        config=environment_config,
        description=environment_description,
        worker_groups=normalized_worker_groups,
    )


def update_environment_result(
    environment: str,
    *,
    name: str | None = None,
    config: str | None = None,
    description: DescriptionUpdate = UNSET,
    worker_groups: WorkerGroupsUpdate = UNSET,
    env_file: str | None = None,
) -> CommandResult:
    """Update one environment while preserving omitted fields."""
    if (
        name is None
        and config is None
        and description is UNSET
        and worker_groups is UNSET
    ):
        message = "Environment update requires at least one field change"
        raise UserInputError(
            message,
            suggestion=(
                "Pass at least one update flag such as --name, --config, "
                "--description, --clear-description, --worker-group, or "
                "--clear-worker-groups."
            ),
        )

    normalized_name = (
        require_non_empty_text(name, label="environment name")
        if name is not None
        else None
    )
    normalized_config = (
        require_non_empty_text(config, label="environment config")
        if config is not None
        else None
    )
    normalized_description = (
        optional_text(description)
        if not isinstance(description, _UnsetValue)
        else UNSET
    )
    normalized_worker_groups = (
        _normalize_worker_groups(worker_groups)
        if not isinstance(worker_groups, _UnsetValue)
        else UNSET
    )

    return run_with_bound_domain_service_runtime(
        env_file,
        ENVIRONMENT_DOMAIN,
        _update_environment_result,
        environment=environment,
        name=normalized_name,
        config=normalized_config,
        description=normalized_description,
        worker_groups=normalized_worker_groups,
    )


def delete_environment_result(
    environment: str,
    *,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one environment after explicit confirmation."""
    require_delete_force(force=force, resource_label="Environment")

    return run_with_bound_domain_service_runtime(
        env_file,
        ENVIRONMENT_DOMAIN,
        _delete_environment_result,
        environment=environment,
    )


def _list_environments_result(
    runtime: BoundDomainServiceRuntime[EnvironmentDomain],
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.domain.environments
    return paged_command_result(
        lambda current_page_no, current_page_size: adapter.list(
            page_no=current_page_no,
            page_size=current_page_size,
            search=search,
        ),
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=serialize_environment,
        resource=ENV_RESOURCE,
        resolved={"search": search},
        translate_error=lambda error: _translate_environment_api_error(
            error,
            operation="list",
            name=search,
        ),
    )


def _get_environment_result(
    runtime: BoundDomainServiceRuntime[EnvironmentDomain],
    *,
    environment: str,
) -> CommandResult:
    adapter = runtime.domain.environments
    try:
        resolved_environment = resolve_environment(
            environment,
            adapter=adapter,
        )
    except ApiResultError as error:
        raise _translate_environment_api_error(
            error,
            operation="get",
            name=environment,
        ) from error
    try:
        fetched_environment = adapter.get(code=resolved_environment.code)
    except ApiResultError as error:
        raise _translate_environment_api_error(
            error,
            operation="get",
            code=resolved_environment.code,
            name=resolved_environment.name,
        ) from error
    return CommandResult(
        data=require_json_object(
            serialize_environment(fetched_environment),
            label="environment data",
        ),
        resolved={
            "environment": require_json_object(
                resolved_environment.to_data(),
                label="resolved environment",
            )
        },
    )


def _create_environment_result(
    runtime: BoundDomainServiceRuntime[EnvironmentDomain],
    *,
    name: str,
    config: str,
    description: str | None,
    worker_groups: list[str] | None,
) -> CommandResult:
    adapter = runtime.domain.environments
    try:
        created_environment = adapter.create(
            name=name,
            config=config,
            description=description,
            worker_groups=worker_groups,
        )
    except ApiResultError as error:
        raise _translate_environment_api_error(
            error,
            operation="create",
            name=name,
        ) from error

    return CommandResult(
        data=require_json_object(
            serialize_environment(created_environment),
            label="environment data",
        ),
        resolved={
            "environment": require_json_object(
                _resolved_environment_data(created_environment),
                label="resolved environment",
            )
        },
    )


def _update_environment_result(
    runtime: BoundDomainServiceRuntime[EnvironmentDomain],
    *,
    environment: str,
    name: str | None,
    config: str | None,
    description: DescriptionUpdate,
    worker_groups: WorkerGroupsUpdate,
) -> CommandResult:
    adapter = runtime.domain.environments
    try:
        resolved_environment = resolve_environment(environment, adapter=adapter)
    except ApiResultError as error:
        raise _translate_environment_api_error(
            error,
            operation="update",
            name=environment,
        ) from error
    try:
        current_environment = adapter.get(code=resolved_environment.code)
    except ApiResultError as error:
        raise _translate_environment_api_error(
            error,
            operation="get",
            code=resolved_environment.code,
            name=resolved_environment.name,
        ) from error

    updated_description = (
        current_environment.description
        if isinstance(description, _UnsetValue)
        else description
    )
    updated_worker_groups = (
        _current_worker_groups(current_environment)
        if isinstance(worker_groups, _UnsetValue)
        else worker_groups
    )
    updated_config = (
        _require_current_environment_config(current_environment)
        if config is None
        else config
    )

    try:
        updated_environment = adapter.update(
            code=resolved_environment.code,
            name=name or resolved_environment.name,
            config=updated_config,
            description=updated_description,
            worker_groups=updated_worker_groups,
        )
    except ApiResultError as error:
        raise _translate_environment_api_error(
            error,
            operation="update",
            code=resolved_environment.code,
            name=name or resolved_environment.name,
        ) from error

    return CommandResult(
        data=require_json_object(
            serialize_environment(updated_environment),
            label="environment data",
        ),
        resolved={
            "environment": require_json_object(
                resolved_environment.to_data(),
                label="resolved environment",
            )
        },
    )


def _delete_environment_result(
    runtime: BoundDomainServiceRuntime[EnvironmentDomain],
    *,
    environment: str,
) -> CommandResult:
    adapter = runtime.domain.environments
    try:
        resolved_environment = resolve_environment(environment, adapter=adapter)
    except ApiResultError as error:
        raise _translate_environment_api_error(
            error,
            operation="delete",
            name=environment,
        ) from error
    try:
        deleted = adapter.delete(code=resolved_environment.code)
    except ApiResultError as error:
        raise _translate_environment_api_error(
            error,
            operation="delete",
            code=resolved_environment.code,
            name=resolved_environment.name,
        ) from error

    return CommandResult(
        data=require_json_object(
            DeleteEnvironmentData(
                deleted=deleted,
                environment=resolved_environment.to_data(),
            ),
            label="environment delete data",
        ),
        resolved={
            "environment": require_json_object(
                resolved_environment.to_data(),
                label="resolved environment",
            )
        },
    )


def _normalize_worker_groups(worker_groups: Sequence[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for worker_group in worker_groups:
        normalized_worker_group = require_non_empty_text(
            worker_group,
            label="worker group",
        )
        if normalized_worker_group not in seen:
            normalized.append(normalized_worker_group)
            seen.add(normalized_worker_group)
    return normalized


def _current_worker_groups(environment: EnvironmentPayloadRecord) -> list[str]:
    if environment.workerGroups is None:
        return []
    return list(environment.workerGroups)


def _require_current_environment_config(environment: EnvironmentPayloadRecord) -> str:
    config = environment.config
    if config is None:
        message = "Fetched environment payload was missing environment config"
        raise ApiTransportError(
            message,
            details={"resource": ENV_RESOURCE, "code": environment.code},
        )
    return config


def _resolved_environment_data(
    environment: EnvironmentPayloadRecord,
) -> ResolvedEnvironmentData:
    code = environment.code
    name = environment.name
    if code is None or name is None:
        message = "Environment payload was missing required identity fields"
        raise ApiTransportError(
            message,
            details={"resource": ENV_RESOURCE},
        )
    return {
        "code": code,
        "name": name,
        "description": environment.description,
    }


def _translate_environment_api_error(
    error: ApiResultError,
    *,
    operation: str,
    code: int | None = None,
    name: str | None = None,
) -> Exception:
    result_code = error.result_code
    details: dict[str, int | str] = {
        "resource": ENV_RESOURCE,
        "operation": operation,
    }
    if code is not None:
        details["code"] = code
    if name is not None:
        details["name"] = name

    if result_code == QUERY_ENVIRONMENT_BY_CODE_ERROR:
        identifier = code if code is not None else name
        message = f"Environment {identifier!r} was not found"
        return NotFoundError(message, details=details)
    if result_code in {
        ENVIRONMENT_NAME_EXISTS,
        DELETE_ENVIRONMENT_RELATED_TASK_EXISTS,
        LEGACY_UPDATE_ENVIRONMENT_WORKER_GROUP_RELATION_ERROR,
        UPDATE_ENVIRONMENT_WORKER_GROUP_RELATION_ERROR,
    }:
        return ConflictError(error.message, details=details)
    if result_code == USER_NO_OPERATION_PERM:
        message = f"Environment {operation} requires additional permissions"
        return PermissionDeniedError(message, details=details)
    if result_code in {
        ENVIRONMENT_NAME_IS_NULL,
        ENVIRONMENT_CONFIG_IS_NULL,
        LEGACY_ENVIRONMENT_WORKER_GROUPS_IS_INVALID,
        ENVIRONMENT_WORKER_GROUPS_IS_INVALID,
    }:
        return UserInputError(
            error.message,
            details=details,
            suggestion=(
                "Verify --name, --config, and --worker-group values, then retry."
            ),
        )
    return error
