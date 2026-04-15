from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias, TypedDict

from dsctl.cli_surface import PROJECT_RESOURCE
from dsctl.errors import ApiTransportError, UserInputError
from dsctl.output import CommandResult, require_json_object
from dsctl.services._serialization import optional_text
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
from dsctl.services.resolver import ResolvedProjectData
from dsctl.services.resolver import project as resolve_project
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime

if TYPE_CHECKING:
    from dsctl.upstream.protocol import (
        ProjectPayloadRecord,
        ProjectRecord,
    )


class ProjectData(TypedDict):
    """JSON object emitted for one project."""

    id: int | None
    userId: int | None
    userName: str | None
    code: int
    name: str | None
    description: str | None
    createTime: str | None
    updateTime: str | None
    perm: int
    defCount: int


class DeleteProjectData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    project: ResolvedProjectData


ProjectPageData: TypeAlias = PageData[ProjectData]


class _UnsetValue:
    """Sentinel for update fields that should keep their current value."""


UNSET = _UnsetValue()
DescriptionUpdate = str | None | _UnsetValue


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

    return run_with_service_runtime(
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
    return run_with_service_runtime(
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

    return run_with_service_runtime(
        env_file,
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

    return run_with_service_runtime(
        env_file,
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

    return run_with_service_runtime(
        env_file,
        _delete_project_result,
        project=project,
    )


def _list_projects_result(
    runtime: ServiceRuntime,
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.upstream.projects
    data: ProjectPageData = requested_page_data(
        # DS project list currently exposes only generic controller/list
        # fallback failures at this boundary, so list-time ApiResultError
        # stays raw until upstream exposes stable domain semantics.
        lambda current_page_no, current_page_size: adapter.list(
            page_no=current_page_no,
            page_size=current_page_size,
            search=search,
        ),
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=_serialize_project,
        resource=PROJECT_RESOURCE,
        max_pages=MAX_AUTO_EXHAUST_PAGES,
    )

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
    runtime: ServiceRuntime,
    *,
    project: str,
) -> CommandResult:
    adapter = runtime.upstream.projects
    resolved_project = resolve_project(
        project,
        adapter=adapter,
    )
    fetched_project = adapter.get(
        code=resolved_project.code,
    )

    return CommandResult(
        data=require_json_object(
            _serialize_project(fetched_project),
            label="project data",
        ),
        resolved={
            "project": require_json_object(
                resolved_project.to_data(),
                label="resolved project",
            )
        },
    )


def _create_project_result(
    runtime: ServiceRuntime,
    *,
    name: str,
    description: str | None,
) -> CommandResult:
    adapter = runtime.upstream.projects
    created_project = adapter.create(
        name=name,
        description=description,
    )

    return CommandResult(
        data=require_json_object(
            _serialize_project(created_project),
            label="project data",
        ),
        resolved={
            "project": require_json_object(
                _resolved_project_data(created_project),
                label="resolved project",
            )
        },
    )


def _update_project_result(
    runtime: ServiceRuntime,
    *,
    project: str,
    name: str | None,
    description: DescriptionUpdate,
) -> CommandResult:
    adapter = runtime.upstream.projects
    resolved_project = resolve_project(
        project,
        adapter=adapter,
    )
    updated_description = (
        resolved_project.description
        if isinstance(description, _UnsetValue)
        else description
    )
    updated_project = adapter.update(
        code=resolved_project.code,
        name=name or resolved_project.name,
        description=updated_description,
    )

    return CommandResult(
        data=require_json_object(
            _serialize_project(updated_project),
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
    runtime: ServiceRuntime,
    *,
    project: str,
) -> CommandResult:
    adapter = runtime.upstream.projects
    resolved_project = resolve_project(
        project,
        adapter=adapter,
    )
    deleted = adapter.delete(
        code=resolved_project.code,
    )

    return CommandResult(
        data=require_json_object(
            DeleteProjectData(
                deleted=deleted,
                project=resolved_project.to_data(),
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


def _serialize_project(project: ProjectPayloadRecord) -> ProjectData:
    return {
        "id": project.id,
        "userId": project.userId,
        "userName": project.userName,
        "code": project.code,
        "name": project.name,
        "description": project.description,
        "createTime": project.createTime,
        "updateTime": project.updateTime,
        "perm": project.perm,
        "defCount": project.defCount,
    }


def _resolved_project_data(project: ProjectRecord) -> ResolvedProjectData:
    if project.code is None or project.name is None:
        message = "Project payload was missing required identity fields"
        raise ApiTransportError(
            message,
            details={"resource": PROJECT_RESOURCE},
        )
    return {
        "code": project.code,
        "name": project.name,
        "description": project.description,
    }
