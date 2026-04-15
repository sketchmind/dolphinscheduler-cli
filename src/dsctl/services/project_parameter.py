from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias, TypedDict

from dsctl.cli_surface import PROJECT_PARAMETER_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConflictError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._serialization import (
    ProjectParameterData,
    optional_text,
    require_resource_text,
    serialize_project_parameter,
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
    ResolvedProjectData,
    ResolvedProjectParameterData,
)
from dsctl.services.resolver import project as resolve_project
from dsctl.services.resolver import project_parameter as resolve_project_parameter
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime
from dsctl.services.selection import (
    SelectedValue,
    require_project_selection,
    with_selection_source,
)

if TYPE_CHECKING:
    from dsctl.upstream.protocol import ProjectParameterRecord

PROJECT_PARAMETER_ALREADY_EXISTS = 10218
PROJECT_PARAMETER_NOT_EXISTS = 10219
PROJECT_PARAMETER_CODE_EMPTY = 10220
USER_NO_OPERATION_PERM = 30001

ProjectParameterPageData: TypeAlias = PageData[ProjectParameterData]


class DeleteProjectParameterData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    projectParameter: ResolvedProjectParameterData


class _UnsetValue:
    """Sentinel for update fields that should keep their current value."""


UNSET = _UnsetValue()
ValueUpdate = str | _UnsetValue


def list_project_parameters_result(
    *,
    project: str | None = None,
    search: str | None = None,
    data_type: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """List project parameters inside one selected project."""
    normalized_search = optional_text(search)
    normalized_data_type = (
        require_non_empty_text(data_type, label="project parameter data type")
        if data_type is not None
        else None
    )
    require_positive_int(page_no, label="page_no")
    require_positive_int(page_size, label="page_size")

    return run_with_service_runtime(
        env_file,
        _list_project_parameters_result,
        project=project,
        search=normalized_search,
        data_type=normalized_data_type,
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
    )


def get_project_parameter_result(
    project_parameter: str,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Resolve and fetch one project parameter inside one selected project."""
    return run_with_service_runtime(
        env_file,
        _get_project_parameter_result,
        project_parameter=project_parameter,
        project=project,
    )


def create_project_parameter_result(
    *,
    name: str,
    value: str,
    data_type: str = "VARCHAR",
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create one project parameter from validated CLI input."""
    parameter_name = require_non_empty_text(name, label="project parameter name")
    parameter_data_type = require_non_empty_text(
        data_type,
        label="project parameter data type",
    )

    return run_with_service_runtime(
        env_file,
        _create_project_parameter_result,
        project=project,
        name=parameter_name,
        value=value,
        data_type=parameter_data_type,
    )


def update_project_parameter_result(
    project_parameter: str,
    *,
    name: str | None = None,
    value: ValueUpdate = UNSET,
    data_type: str | None = None,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Update one project parameter while preserving omitted fields."""
    if name is None and isinstance(value, _UnsetValue) and data_type is None:
        message = "Project parameter update requires at least one field change"
        raise UserInputError(
            message,
            suggestion=(
                "Pass at least one update flag such as --name, --value, or --data-type."
            ),
        )

    normalized_name = (
        require_non_empty_text(name, label="project parameter name")
        if name is not None
        else None
    )
    normalized_data_type = (
        require_non_empty_text(data_type, label="project parameter data type")
        if data_type is not None
        else None
    )

    return run_with_service_runtime(
        env_file,
        _update_project_parameter_result,
        project_parameter=project_parameter,
        project=project,
        name=normalized_name,
        value=value,
        data_type=normalized_data_type,
    )


def delete_project_parameter_result(
    project_parameter: str,
    *,
    project: str | None = None,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one project parameter after explicit confirmation."""
    require_delete_force(force=force, resource_label="Project parameter")

    return run_with_service_runtime(
        env_file,
        _delete_project_parameter_result,
        project_parameter=project_parameter,
        project=project,
    )


def _list_project_parameters_result(
    runtime: ServiceRuntime,
    *,
    project: str | None,
    search: str | None,
    data_type: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    adapter = runtime.upstream.project_parameters
    data: ProjectParameterPageData = requested_page_data(
        lambda current_page_no, current_page_size: adapter.list(
            project_code=resolved_project.code,
            page_no=current_page_no,
            page_size=current_page_size,
            search=search,
            data_type=data_type,
        ),
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=serialize_project_parameter,
        resource=PROJECT_PARAMETER_RESOURCE,
        max_pages=MAX_AUTO_EXHAUST_PAGES,
        translate_error=lambda error: _translate_project_parameter_api_error(
            error,
            project_code=resolved_project.code,
        ),
    )

    return CommandResult(
        data=require_json_object(data, label="project parameter list data"),
        resolved={
            "project": require_json_object(
                _selected_project_data(resolved_project.to_data(), selected_project),
                label="resolved project",
            ),
            "search": search,
            "data_type": data_type,
            "page_no": page_no,
            "page_size": page_size,
            "all": all_pages,
        },
    )


def _get_project_parameter_result(
    runtime: ServiceRuntime,
    *,
    project_parameter: str,
    project: str | None,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    resolved_parameter = resolve_project_parameter(
        project_parameter,
        adapter=runtime.upstream.project_parameters,
        project_code=resolved_project.code,
    )
    fetched_parameter = _get_project_parameter(
        runtime,
        project_code=resolved_project.code,
        code=resolved_parameter.code,
    )
    return CommandResult(
        data=require_json_object(
            serialize_project_parameter(fetched_parameter),
            label="project parameter data",
        ),
        resolved={
            "project": require_json_object(
                _selected_project_data(resolved_project.to_data(), selected_project),
                label="resolved project",
            ),
            "projectParameter": require_json_object(
                resolved_parameter.to_data(),
                label="resolved project parameter",
            ),
        },
    )


def _create_project_parameter_result(
    runtime: ServiceRuntime,
    *,
    project: str | None,
    name: str,
    value: str,
    data_type: str,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    try:
        created_parameter = runtime.upstream.project_parameters.create(
            project_code=resolved_project.code,
            name=name,
            value=value,
            data_type=data_type,
        )
    except ApiResultError as error:
        raise _translate_project_parameter_api_error(
            error,
            project_code=resolved_project.code,
            name=name,
        ) from error

    return CommandResult(
        data=require_json_object(
            serialize_project_parameter(created_parameter),
            label="project parameter data",
        ),
        resolved={
            "project": require_json_object(
                _selected_project_data(resolved_project.to_data(), selected_project),
                label="resolved project",
            ),
            "projectParameter": require_json_object(
                _resolved_project_parameter_data(created_parameter),
                label="resolved project parameter",
            ),
        },
    )


def _update_project_parameter_result(
    runtime: ServiceRuntime,
    *,
    project_parameter: str,
    project: str | None,
    name: str | None,
    value: ValueUpdate,
    data_type: str | None,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    resolved_parameter = resolve_project_parameter(
        project_parameter,
        adapter=runtime.upstream.project_parameters,
        project_code=resolved_project.code,
    )
    current_parameter = _get_project_parameter(
        runtime,
        project_code=resolved_project.code,
        code=resolved_parameter.code,
    )
    updated_name = (
        name
        if name is not None
        else require_resource_text(
            current_parameter.paramName,
            resource=PROJECT_PARAMETER_RESOURCE,
            field_name="paramName",
        )
    )
    updated_value = (
        value
        if not isinstance(value, _UnsetValue)
        else _require_project_parameter_value(current_parameter.paramValue)
    )
    updated_data_type = (
        data_type
        if data_type is not None
        else require_resource_text(
            current_parameter.paramDataType,
            resource=PROJECT_PARAMETER_RESOURCE,
            field_name="paramDataType",
        )
    )

    try:
        updated_parameter = runtime.upstream.project_parameters.update(
            project_code=resolved_project.code,
            code=resolved_parameter.code,
            name=updated_name,
            value=updated_value,
            data_type=updated_data_type,
        )
    except ApiResultError as error:
        raise _translate_project_parameter_api_error(
            error,
            project_code=resolved_project.code,
            code=resolved_parameter.code,
            name=updated_name,
        ) from error

    return CommandResult(
        data=require_json_object(
            serialize_project_parameter(updated_parameter),
            label="project parameter data",
        ),
        resolved={
            "project": require_json_object(
                _selected_project_data(resolved_project.to_data(), selected_project),
                label="resolved project",
            ),
            "projectParameter": require_json_object(
                resolved_parameter.to_data(),
                label="resolved project parameter",
            ),
        },
    )


def _delete_project_parameter_result(
    runtime: ServiceRuntime,
    *,
    project_parameter: str,
    project: str | None,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    resolved_parameter = resolve_project_parameter(
        project_parameter,
        adapter=runtime.upstream.project_parameters,
        project_code=resolved_project.code,
    )
    try:
        deleted = runtime.upstream.project_parameters.delete(
            project_code=resolved_project.code,
            code=resolved_parameter.code,
        )
    except ApiResultError as error:
        raise _translate_project_parameter_api_error(
            error,
            project_code=resolved_project.code,
            code=resolved_parameter.code,
        ) from error

    return CommandResult(
        data=require_json_object(
            DeleteProjectParameterData(
                deleted=deleted,
                projectParameter=resolved_parameter.to_data(),
            ),
            label="project parameter delete data",
        ),
        resolved={
            "project": require_json_object(
                _selected_project_data(resolved_project.to_data(), selected_project),
                label="resolved project",
            ),
            "projectParameter": require_json_object(
                resolved_parameter.to_data(),
                label="resolved project parameter",
            ),
        },
    )


def _get_project_parameter(
    runtime: ServiceRuntime,
    *,
    project_code: int,
    code: int,
) -> ProjectParameterRecord:
    try:
        return runtime.upstream.project_parameters.get(
            project_code=project_code,
            code=code,
        )
    except ApiResultError as error:
        raise _translate_project_parameter_api_error(
            error,
            project_code=project_code,
            code=code,
        ) from error


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


def _require_project_parameter_value(value: str | None) -> str:
    if value is None:
        message = "Project Parameter payload was missing required field 'paramValue'"
        raise ApiTransportError(
            message,
            details={
                "resource": PROJECT_PARAMETER_RESOURCE,
                "field": "paramValue",
            },
        )
    return value


def _resolved_project_parameter_data(
    project_parameter: ProjectParameterRecord,
) -> ResolvedProjectParameterData:
    if project_parameter.code is None or project_parameter.paramName is None:
        message = (
            "Resolved project parameter payload was missing required identity fields"
        )
        raise ApiTransportError(
            message,
            details={"resource": PROJECT_PARAMETER_RESOURCE},
        )
    return {
        "code": project_parameter.code,
        "paramName": project_parameter.paramName,
        "paramDataType": project_parameter.paramDataType,
    }


def _translate_project_parameter_api_error(
    error: ApiResultError,
    *,
    project_code: int,
    code: int | None = None,
    name: str | None = None,
) -> ApiResultError | ConflictError | NotFoundError | PermissionDeniedError:
    details: dict[str, object] = {
        "resource": PROJECT_PARAMETER_RESOURCE,
        "project_code": project_code,
    }
    if code is not None:
        details["code"] = code
    if name is not None:
        details["name"] = name

    if error.result_code == PROJECT_PARAMETER_ALREADY_EXISTS:
        message = (
            "Project parameter already exists"
            if name is None
            else f"Project parameter {name!r} already exists"
        )
        return ConflictError(message, details=details)

    if error.result_code in {
        PROJECT_PARAMETER_NOT_EXISTS,
        PROJECT_PARAMETER_CODE_EMPTY,
    }:
        if code is not None:
            message = f"Project parameter code {code} was not found"
        elif name is not None:
            message = f"Project parameter {name!r} was not found"
        else:
            message = "Project parameter was not found"
        return NotFoundError(message, details=details)

    if error.result_code == USER_NO_OPERATION_PERM:
        return PermissionDeniedError(
            "Current user does not have permission to access project parameters",
            details=details,
        )

    return error
