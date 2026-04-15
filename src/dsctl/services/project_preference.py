from __future__ import annotations

import json
from typing import TYPE_CHECKING, TypedDict

from dsctl.cli_surface import PROJECT_PREFERENCE_RESOURCE, PROJECT_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ConflictError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._serialization import (
    serialize_project_preference,
)
from dsctl.services._validation import require_non_empty_text
from dsctl.services.resolver import ResolvedProject, ResolvedProjectData
from dsctl.services.resolver import project as resolve_project
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime
from dsctl.services.selection import (
    SelectedValue,
    require_project_selection,
    with_selection_source,
)

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.upstream.protocol import ProjectPreferenceRecord


PROJECT_NOT_FOUND = 10018
PROJECT_NOT_EXIST = 10190
USER_NO_OPERATION_PERM = 30001
CREATE_PROJECT_PREFERENCE_ERROR = 10300
UPDATE_PROJECT_PREFERENCE_ERROR = 10301
QUERY_PROJECT_PREFERENCE_ERROR = 10302
UPDATE_PROJECT_PREFERENCE_STATE_ERROR = 10303
PROJECT_PREFERENCE_ENABLED = 1
PROJECT_PREFERENCE_DISABLED = 0


class ProjectPreferenceStateWarningDetail(TypedDict):
    """Warning detail emitted when DS accepts a state change but stores nothing."""

    code: str
    message: str
    projectCode: int
    requestedState: int


def get_project_preference_result(
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Fetch the selected project preference default-value source."""
    return run_with_service_runtime(
        env_file,
        _get_project_preference_result,
        project=project,
    )


def update_project_preference_result(
    *,
    project: str | None = None,
    preferences_json: str | None = None,
    file: Path | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create or update the selected project preference default-value source."""
    preferences = _preferences_payload(preferences_json=preferences_json, file=file)
    return run_with_service_runtime(
        env_file,
        _update_project_preference_result,
        project=project,
        preferences=preferences,
    )


def enable_project_preference_result(
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Enable the selected project preference default-value source."""
    return run_with_service_runtime(
        env_file,
        _set_project_preference_state_result,
        project=project,
        state=PROJECT_PREFERENCE_ENABLED,
    )


def disable_project_preference_result(
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Disable the selected project preference default-value source."""
    return run_with_service_runtime(
        env_file,
        _set_project_preference_state_result,
        project=project,
        state=PROJECT_PREFERENCE_DISABLED,
    )


def _get_project_preference_result(
    runtime: ServiceRuntime,
    *,
    project: str | None,
) -> CommandResult:
    selected_project, resolved_project = _selected_project(runtime, project=project)
    payload = _get_project_preference(
        runtime,
        project_code=resolved_project.code,
    )
    data = (
        None
        if payload is None
        else require_json_object(
            serialize_project_preference(payload),
            label="project preference data",
        )
    )
    return CommandResult(
        data=data,
        resolved={
            "project": require_json_object(
                _selected_project_data(resolved_project.to_data(), selected_project),
                label="resolved project",
            )
        },
    )


def _update_project_preference_result(
    runtime: ServiceRuntime,
    *,
    project: str | None,
    preferences: str,
) -> CommandResult:
    selected_project, resolved_project = _selected_project(runtime, project=project)
    try:
        payload = runtime.upstream.project_preferences.update(
            project_code=resolved_project.code,
            preferences=preferences,
        )
    except ApiResultError as error:
        raise _translate_project_preference_api_error(
            error,
            project_code=resolved_project.code,
            operation="update",
        ) from error
    return CommandResult(
        data=require_json_object(
            serialize_project_preference(payload),
            label="project preference data",
        ),
        resolved={
            "project": require_json_object(
                _selected_project_data(resolved_project.to_data(), selected_project),
                label="resolved project",
            )
        },
    )


def _set_project_preference_state_result(
    runtime: ServiceRuntime,
    *,
    project: str | None,
    state: int,
) -> CommandResult:
    selected_project, resolved_project = _selected_project(runtime, project=project)
    operation = "enable" if state == PROJECT_PREFERENCE_ENABLED else "disable"
    try:
        runtime.upstream.project_preferences.set_state(
            project_code=resolved_project.code,
            state=state,
        )
    except ApiResultError as error:
        raise _translate_project_preference_api_error(
            error,
            project_code=resolved_project.code,
            operation=operation,
        ) from error

    payload = _get_project_preference(runtime, project_code=resolved_project.code)
    warning = _missing_project_preference_warning(
        project_code=resolved_project.code,
        requested_state=state,
        payload=payload,
    )
    data = (
        None
        if payload is None
        else require_json_object(
            serialize_project_preference(payload),
            label="project preference data",
        )
    )
    warnings = [] if warning is None else [warning[0]]
    warning_details = [] if warning is None else [warning[1]]
    return CommandResult(
        data=data,
        resolved={
            "project": require_json_object(
                _selected_project_data(resolved_project.to_data(), selected_project),
                label="resolved project",
            )
        },
        warnings=warnings,
        warning_details=warning_details,
    )


def _selected_project(
    runtime: ServiceRuntime,
    *,
    project: str | None,
) -> tuple[SelectedValue, ResolvedProject]:
    selected_project = require_project_selection(project, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    return selected_project, resolved_project


def _get_project_preference(
    runtime: ServiceRuntime,
    *,
    project_code: int,
) -> ProjectPreferenceRecord | None:
    try:
        return runtime.upstream.project_preferences.get(project_code=project_code)
    except ApiResultError as error:
        raise _translate_project_preference_api_error(
            error,
            project_code=project_code,
            operation="get",
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


def _preferences_payload(
    *,
    preferences_json: str | None,
    file: Path | None,
) -> str:
    if (preferences_json is None) == (file is None):
        message = "Project preference update requires exactly one input source"
        raise UserInputError(
            message,
            suggestion="Pass exactly one of --preferences-json or --file.",
        )
    if preferences_json is not None:
        return _normalize_preferences_text(
            require_non_empty_text(
                preferences_json,
                label="project preference JSON",
            ),
            source="--preferences-json",
        )
    if file is None:
        message = "Project preference input source is missing"
        raise RuntimeError(message)
    return _preferences_payload_from_file(file)


def _preferences_payload_from_file(file: Path) -> str:
    try:
        raw_text = file.read_text(encoding="utf-8")
    except OSError as error:
        message = f"Project preference file {str(file)!r} could not be read"
        raise UserInputError(
            message,
            details={"path": str(file)},
            suggestion="Verify the --file path exists and is readable, then retry.",
        ) from error
    return _normalize_preferences_text(raw_text, source=str(file))


def _normalize_preferences_text(raw_text: str, *, source: str) -> str:
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError as error:
        message = "Project preference payload must be valid JSON"
        raise UserInputError(
            message,
            details={
                "source": source,
                "line": error.lineno,
                "column": error.colno,
            },
            suggestion=(
                "Pass one JSON object such as "
                """'{"taskPriority":"HIGH"}'."""
            ),
        ) from error
    try:
        payload = require_json_object(parsed, label="project preference payload")
    except TypeError as error:
        message = "Project preference payload must be a JSON object"
        raise UserInputError(
            message,
            details={"source": source},
            suggestion=(
                "Pass one JSON object such as "
                """'{"taskPriority":"HIGH"}'."""
            ),
        ) from error
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def _missing_project_preference_warning(
    *,
    project_code: int,
    requested_state: int,
    payload: ProjectPreferenceRecord | None,
) -> tuple[str, ProjectPreferenceStateWarningDetail] | None:
    if payload is not None:
        return None
    warning = (
        "Project preference state update was accepted, but the project has no "
        "stored project preference."
    )
    return (
        warning,
        {
            "code": "project_preference_missing",
            "message": warning,
            "projectCode": project_code,
            "requestedState": requested_state,
        },
    )


def _translate_project_preference_api_error(
    error: ApiResultError,
    *,
    project_code: int,
    operation: str,
) -> Exception:
    details: dict[str, object] = {
        "resource": PROJECT_PREFERENCE_RESOURCE,
        "project_code": project_code,
        "operation": operation,
    }
    if error.result_code in (PROJECT_NOT_FOUND, PROJECT_NOT_EXIST):
        return NotFoundError(
            f"Project code {project_code} was not found",
            details={"resource": PROJECT_RESOURCE, "code": project_code},
        )
    if error.result_code == USER_NO_OPERATION_PERM:
        return PermissionDeniedError(
            "Project preference operation requires additional permissions",
            details=details,
        )
    if error.result_code == QUERY_PROJECT_PREFERENCE_ERROR:
        return ConflictError(
            "Project preference query was rejected by the upstream API",
            details=details,
        )
    if error.result_code in (
        CREATE_PROJECT_PREFERENCE_ERROR,
        UPDATE_PROJECT_PREFERENCE_ERROR,
    ):
        return ConflictError(
            "Project preference update was rejected by the upstream API",
            details=details,
        )
    if error.result_code == UPDATE_PROJECT_PREFERENCE_STATE_ERROR:
        return ConflictError(
            "Project preference state update was rejected by the upstream API",
            details=details,
        )
    return error
