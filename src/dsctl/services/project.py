from __future__ import annotations

import shlex
from typing import TYPE_CHECKING, TypedDict

from dsctl.cli_surface import PROJECT_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConflictError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._validation import (
    require_delete_force,
    require_non_empty_text,
    require_positive_int,
)
from dsctl.services.runtime import (
    BoundDomainServiceRuntime,
    ReadServiceRuntime,
    run_with_bound_domain_service_runtime,
    run_with_read_service_runtime,
)
from dsctl.upstream.definition_models import NativeIdentity, ProjectRef
from dsctl.upstream.pagination import DEFAULT_PAGE_SIZE
from dsctl.upstream.projects import PROJECT_DOMAIN, ProjectDomain
from dsctl.upstream.serialization import optional_text

if TYPE_CHECKING:
    from dsctl.output import JsonObject


class DeleteProjectData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    project: JsonObject


class _UnsetValue:
    """Sentinel for update fields that should keep their current value."""


UNSET = _UnsetValue()
DescriptionUpdate = str | None | _UnsetValue

PROJECT_NOT_FOUND = 10018
PROJECT_ALREADY_EXISTS = 10019
DELETE_PROJECT_ERROR_DEFINES_NOT_NULL = 10137
PROJECT_NOT_EXIST = 10190
USER_NO_OPERATION_PERM = 30001
USER_NO_OPERATION_PROJECT_PERM = 30002
USER_NO_WRITE_PROJECT_PERM = 30003
DESCRIPTION_TOO_LONG_ERROR = 1400004


def list_projects_result(
    *,
    env_file: str | None = None,
    search: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
) -> CommandResult:
    """List projects with explicit paging or auto-exhaust support."""
    normalized_search = optional_text(search)
    require_positive_int(page_no, label="page_no")
    require_positive_int(page_size, label="page_size")

    return run_with_read_service_runtime(
        env_file,
        _list_projects_result,
        search=normalized_search,
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
    )


def get_project_result(
    project: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Resolve and fetch a single project."""
    return run_with_read_service_runtime(
        env_file,
        _get_project_result,
        project=project,
    )


def create_project_result(
    *,
    name: str,
    description: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create a project from validated CLI input."""
    project_name = require_non_empty_text(name, label="project name")
    project_description = optional_text(description)

    return run_with_bound_domain_service_runtime(
        env_file,
        PROJECT_DOMAIN,
        _create_project_result,
        name=project_name,
        description=project_description,
    )


def update_project_result(
    project: str,
    *,
    name: str | None = None,
    description: DescriptionUpdate = UNSET,
    env_file: str | None = None,
) -> CommandResult:
    """Update an existing project while preserving omitted values."""
    if name is None and description is UNSET:
        message = "Project update requires at least one field change"
        raise UserInputError(
            message,
            suggestion="Pass at least one update flag such as --name or --description.",
        )

    new_name = optional_text(name)
    if new_name is not None:
        new_name = require_non_empty_text(new_name, label="project name")
    normalized_description = (
        optional_text(description)
        if not isinstance(description, _UnsetValue)
        else UNSET
    )

    return run_with_bound_domain_service_runtime(
        env_file,
        PROJECT_DOMAIN,
        _update_project_result,
        project=project,
        name=new_name,
        description=normalized_description,
    )


def delete_project_result(
    project: str,
    *,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete a project after explicit confirmation."""
    require_delete_force(force=force, resource_label="Project")

    return run_with_bound_domain_service_runtime(
        env_file,
        PROJECT_DOMAIN,
        _delete_project_result,
        project=project,
    )


def _list_projects_result(
    runtime: ReadServiceRuntime,
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    try:
        page = runtime.upstream.definitions.list_projects(
            page_no=page_no,
            page_size=page_size,
            search=search,
            all_pages=all_pages,
        )
    except ApiResultError as error:
        raise _translate_project_api_error(error, operation="list") from error
    data = page.to_data(lambda project: project.to_data())

    return CommandResult(
        data=require_json_object(data, label="project list data"),
        resolved={
            "search": search,
            "page_no": page_no,
            "page_size": page_size,
            "all": all_pages,
        },
    )


def _get_project_result(
    runtime: ReadServiceRuntime,
    *,
    project: str,
) -> CommandResult:
    try:
        project_read = runtime.upstream.definitions.get_project(project)
    except ApiResultError as error:
        raise _translate_project_api_error(
            error,
            operation="get",
            name=project,
        ) from error

    return CommandResult(
        data=require_json_object(
            project_read.view.to_data(),
            label="project data",
        ),
        resolved={
            "project": require_json_object(
                project_read.project.to_data(),
                label="resolved project",
            )
        },
    )


def _create_project_result(
    runtime: BoundDomainServiceRuntime[ProjectDomain],
    *,
    name: str,
    description: str | None,
) -> CommandResult:
    try:
        created_project = runtime.domain.mutations.create(
            name=name,
            description=description,
        )
    except ApiResultError as error:
        raise _translate_project_api_error(
            error,
            operation="create",
            name=name,
        ) from error

    return CommandResult(
        data=require_json_object(
            created_project.to_data(),
            label="project data",
        ),
        resolved={
            "project": require_json_object(
                created_project.ref.to_data(),
                label="resolved project",
            )
        },
    )


def _update_project_result(
    runtime: BoundDomainServiceRuntime[ProjectDomain],
    *,
    project: str,
    name: str | None,
    description: DescriptionUpdate,
) -> CommandResult:
    current = runtime.domain.definitions.get_project(project)
    resolved_project = current.project
    updated_description = (
        resolved_project.description
        if isinstance(description, _UnsetValue)
        else description
    )
    updated_name = name or _required_project_name(resolved_project)
    try:
        updated_project = runtime.domain.mutations.update(
            native=resolved_project.native,
            name=updated_name,
            description=updated_description,
        )
    except ApiResultError as error:
        raise _translate_project_api_error(
            error,
            operation="update",
            native=resolved_project.native,
            name=updated_name,
        ) from error

    return CommandResult(
        data=require_json_object(
            updated_project.to_data(),
            label="project data",
        ),
        resolved={
            "project": require_json_object(
                resolved_project.to_data(),
                label="resolved project",
            )
        },
    )


def _delete_project_result(
    runtime: BoundDomainServiceRuntime[ProjectDomain],
    *,
    project: str,
) -> CommandResult:
    resolved_project = runtime.domain.definitions.resolve_project(project)
    try:
        deleted = runtime.domain.mutations.delete(
            native=resolved_project.native,
        )
    except ApiResultError as error:
        raise _translate_project_api_error(
            error,
            operation="delete",
            native=resolved_project.native,
            name=resolved_project.name,
        ) from error

    return CommandResult(
        data=require_json_object(
            DeleteProjectData(
                deleted=deleted,
                project=dict(resolved_project.to_data()),
            ),
            label="project delete data",
        ),
        resolved={
            "project": require_json_object(
                resolved_project.to_data(),
                label="resolved project",
            )
        },
    )


def _required_project_name(project: ProjectRef) -> str:
    if project.name is not None:
        return project.name
    message = "Project payload was missing its required name"
    raise ApiTransportError(
        message,
        details={"resource": PROJECT_RESOURCE},
    )


def _translate_project_api_error(
    error: ApiResultError,
    *,
    operation: str,
    native: NativeIdentity | None = None,
    name: str | None = None,
) -> Exception:
    details: dict[str, str | int] = {
        "resource": PROJECT_RESOURCE,
        "operation": operation,
    }
    if native is not None:
        identity_data = ProjectRef(
            native=native,
            name=None,
            description=None,
        ).to_data()
        details.update(
            {
                key: value
                for key, value in identity_data.items()
                if key in {"id", "code"} and isinstance(value, int)
            }
        )
    if name is not None:
        details["name"] = name

    if error.result_code in {PROJECT_NOT_FOUND, PROJECT_NOT_EXIST}:
        identifier = native.value if native is not None else name
        return NotFoundError(
            f"Project {identifier!r} was not found",
            details=details,
        )
    if error.result_code == PROJECT_ALREADY_EXISTS:
        discovery_command = shlex.join(
            ["dsctl", "project", "list", "--search", name or ""],
        )
        return ConflictError(
            f"Project name {name!r} already exists",
            details=details,
            suggestion=(
                f"Run `{discovery_command}` to inspect the existing project, then "
                "retry with a unique --name."
            ),
        )
    if error.result_code in {
        USER_NO_OPERATION_PERM,
        USER_NO_OPERATION_PROJECT_PERM,
        USER_NO_WRITE_PROJECT_PERM,
    }:
        return PermissionDeniedError(
            f"Project {operation} requires additional permissions",
            details=details,
            suggestion=(
                "Ask a DolphinScheduler administrator or project owner to grant "
                "the required project permission, then retry."
            ),
        )
    if error.result_code == DESCRIPTION_TOO_LONG_ERROR:
        return UserInputError(
            "Project description exceeds the DolphinScheduler limit",
            details=details,
            suggestion=(
                "Shorten --description to at most 255 Unicode characters, then retry."
            ),
        )
    if error.result_code == DELETE_PROJECT_ERROR_DEFINES_NOT_NULL:
        return ConflictError(
            "Project still contains workflows and cannot be deleted",
            details=details,
            suggestion=(
                "Delete every workflow in the project, then retry project deletion."
            ),
        )
    return error
