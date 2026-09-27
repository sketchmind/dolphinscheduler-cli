from __future__ import annotations

import re
from typing import TYPE_CHECKING

from dsctl.cli_surface import PROJECT_WORKER_GROUP_RESOURCE, WORKER_GROUP_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ConflictError,
    NotFoundError,
    PermissionDeniedError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.output import CommandResult, require_json_object, require_json_value
from dsctl.services._project_scope import (
    resolve_code_project,
    selected_project_data,
)
from dsctl.services._validation import require_delete_force, require_non_empty_text
from dsctl.services.runtime import (
    BoundDomainServiceRuntime,
    run_with_bound_domain_service_runtime,
)
from dsctl.upstream.project_worker_groups import (
    PROJECT_WORKER_GROUP_DOMAIN,
    ProjectWorkerGroupDomain,
)
from dsctl.upstream.serialization import (
    ProjectWorkerGroupData,
    serialize_project_worker_group,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.upstream.protocol import ProjectWorkerGroupRecord


USER_NO_OPERATION_PERM = 30001
WORKER_GROUP_NOT_EXIST = 1402001
ASSIGN_WORKER_GROUP_TO_PROJECT_ERROR = 1402002
WORKER_GROUP_TO_PROJECT_IS_EMPTY = 1402003
USED_WORKER_GROUP_EXISTS = 1402004

_BRACKETED_NAMES_PATTERN = re.compile(r"\[(?P<items>.*)\]")


def list_project_worker_groups_result(
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """List the worker groups currently reported for one selected project."""
    return run_with_bound_domain_service_runtime(
        env_file,
        PROJECT_WORKER_GROUP_DOMAIN,
        _list_project_worker_groups_result,
        project=project,
    )


def set_project_worker_groups_result(
    *,
    worker_groups: Sequence[str],
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Replace the explicit worker-group assignment set for one selected project."""
    normalized_worker_groups = _normalize_worker_group_names(worker_groups)
    if not normalized_worker_groups:
        message = "Project worker-group set requires at least one --worker-group"
        raise UserInputError(
            message,
            suggestion=(
                "Supply at least one --worker-group. To remove all explicit "
                "assignments, check `dsctl capabilities --action "
                "project-worker-group.clear` before using "
                "`dsctl project-worker-group clear --force`."
            ),
        )
    return run_with_bound_domain_service_runtime(
        env_file,
        PROJECT_WORKER_GROUP_DOMAIN,
        _set_project_worker_groups_result,
        project=project,
        worker_groups=normalized_worker_groups,
    )


def clear_project_worker_groups_result(
    *,
    project: str | None = None,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Clear the explicit worker-group assignment set for one selected project."""
    require_delete_force(force=force, resource_label="Project worker-group")
    return run_with_bound_domain_service_runtime(
        env_file,
        PROJECT_WORKER_GROUP_DOMAIN,
        _set_project_worker_groups_result,
        project=project,
        worker_groups=[],
    )


def _list_project_worker_groups_result(
    runtime: BoundDomainServiceRuntime[ProjectWorkerGroupDomain],
    *,
    project: str | None,
) -> CommandResult:
    selected_project, resolved_project, project_code = resolve_code_project(
        project,
        runtime=runtime,
        definitions=runtime.domain.definitions,
    )
    current_worker_groups = _current_project_worker_groups(
        runtime,
        project_code=project_code,
    )
    return CommandResult(
        data=require_json_value(
            _project_worker_group_data(current_worker_groups),
            label="project worker-group data",
        ),
        resolved={
            "project": require_json_object(
                selected_project_data(resolved_project, selected_project),
                label="resolved project",
            )
        },
    )


def _set_project_worker_groups_result(
    runtime: BoundDomainServiceRuntime[ProjectWorkerGroupDomain],
    *,
    project: str | None,
    worker_groups: Sequence[str],
) -> CommandResult:
    selected_project, resolved_project, project_code = resolve_code_project(
        project,
        runtime=runtime,
        definitions=runtime.domain.definitions,
    )
    try:
        runtime.domain.worker_groups.set(
            project_code=project_code,
            worker_groups=worker_groups,
        )
    except ApiResultError as error:
        raise _translate_project_worker_group_api_error(
            error,
            project_code=project_code,
            worker_groups=worker_groups,
        ) from error
    current_worker_groups = _current_project_worker_groups(
        runtime,
        project_code=project_code,
    )
    current_worker_group_names = _current_worker_group_names(current_worker_groups)
    unexpected_worker_groups = [
        worker_group
        for worker_group in current_worker_group_names
        if worker_group not in set(worker_groups)
    ]
    warning = _retained_worker_group_warning(
        requested_worker_groups=worker_groups,
        unexpected_worker_groups=unexpected_worker_groups,
    )
    warnings = [] if warning is None else [warning[0]]
    warning_details = [] if warning is None else [warning[1]]

    return CommandResult(
        data=require_json_value(
            _project_worker_group_data(current_worker_groups),
            label="project worker-group data",
        ),
        resolved={
            "project": require_json_object(
                selected_project_data(resolved_project, selected_project),
                label="resolved project",
            ),
            "requested_worker_groups": list(worker_groups),
        },
        warnings=warnings,
        warning_details=warning_details,
    )


def _current_project_worker_groups(
    runtime: BoundDomainServiceRuntime[ProjectWorkerGroupDomain],
    *,
    project_code: int,
) -> Sequence[ProjectWorkerGroupRecord]:
    return runtime.domain.worker_groups.list(project_code=project_code)


def _project_worker_group_data(
    worker_groups: Sequence[ProjectWorkerGroupRecord],
) -> list[ProjectWorkerGroupData]:
    return [
        serialize_project_worker_group(worker_group) for worker_group in worker_groups
    ]


def _normalize_worker_group_names(worker_groups: Sequence[str]) -> list[str]:
    normalized_worker_groups: list[str] = []
    seen: set[str] = set()
    for worker_group in worker_groups:
        normalized = require_non_empty_text(
            worker_group,
            label="worker group name",
        )
        if normalized in seen:
            continue
        seen.add(normalized)
        normalized_worker_groups.append(normalized)
    return normalized_worker_groups


def _current_worker_group_names(
    worker_groups: Sequence[ProjectWorkerGroupRecord],
) -> list[str]:
    names: list[str] = []
    for worker_group in worker_groups:
        name = worker_group.workerGroup
        if name is None:
            continue
        names.append(name)
    return names


def _retained_worker_group_warning(
    *,
    requested_worker_groups: Sequence[str],
    unexpected_worker_groups: Sequence[str],
) -> tuple[str, dict[str, object]] | None:
    if not unexpected_worker_groups:
        return None
    warning = "Project still reports worker groups that are used by tasks or schedules."
    return (
        warning,
        {
            "code": "project_worker_group_still_in_use",
            "message": warning,
            "requestedWorkerGroups": list(requested_worker_groups),
            "retainedWorkerGroups": list(unexpected_worker_groups),
        },
    )


def _translate_project_worker_group_api_error(
    error: ApiResultError,
    *,
    project_code: int,
    worker_groups: Sequence[str],
) -> Exception:
    details: dict[str, object] = {
        "resource": PROJECT_WORKER_GROUP_RESOURCE,
        "project_code": project_code,
        "worker_groups": list(worker_groups),
    }
    if error.result_code == USER_NO_OPERATION_PERM:
        return PermissionDeniedError(
            "Project worker-group mutation requires an administrator account",
            details=details,
            suggestion="Select a context with an administrator account and retry.",
        )
    if error.result_code == USED_WORKER_GROUP_EXISTS:
        return ConflictError(
            "Some worker groups are still used by tasks or schedules in the project",
            details={
                **details,
                "used_worker_groups": _bracketed_worker_group_names(error.message),
            },
            suggestion=(
                "Update the worker-group selections in the project's tasks and "
                "schedules before removing those assignments."
            ),
        )
    if error.result_code == WORKER_GROUP_NOT_EXIST:
        missing_worker_groups = _bracketed_worker_group_names(error.message)
        if len(missing_worker_groups) == 1:
            return NotFoundError(
                f"Worker group {missing_worker_groups[0]!r} was not found",
                details={
                    "resource": WORKER_GROUP_RESOURCE,
                    "name": missing_worker_groups[0],
                },
            )
        return NotFoundError(
            "One or more worker groups were not found",
            details={
                "resource": WORKER_GROUP_RESOURCE,
                "worker_groups": missing_worker_groups,
            },
        )
    if error.result_code == WORKER_GROUP_TO_PROJECT_IS_EMPTY:
        return UnsupportedFeatureError(
            "The server requires at least one project worker group and cannot "
            "clear all assignments",
            details=details,
            suggestion=(
                "Keep a nonempty assignment on this server. Clearing all "
                "assignments requires a server release that supports "
                "`project-worker-group.clear`."
            ),
        )
    if error.result_code == ASSIGN_WORKER_GROUP_TO_PROJECT_ERROR:
        return ConflictError(
            "Project worker-group assignment was rejected by the upstream API",
            details=details,
            suggestion=(
                f"Inspect `dsctl project-worker-group list --project {project_code}` "
                "and the server error before deciding whether to retry."
            ),
        )
    return error


def _bracketed_worker_group_names(message: str) -> list[str]:
    matched = _BRACKETED_NAMES_PATTERN.search(message)
    if matched is None:
        return []
    items = matched.group("items").strip()
    if not items:
        return []
    return [item.strip() for item in items.split(",") if item.strip()]
