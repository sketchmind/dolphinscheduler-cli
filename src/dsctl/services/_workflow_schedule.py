from __future__ import annotations

from typing import TYPE_CHECKING, Literal, NoReturn, TypeAlias

from dsctl.cli_surface import SCHEDULE_RESOURCE, WORKFLOW_RESOURCE
from dsctl.errors import (
    ApiHttpError,
    ApiResultError,
    ApiTransportError,
    NotFoundError,
    PermissionDeniedError,
)
from dsctl.services._serialization import enum_value

if TYPE_CHECKING:
    from dsctl.support.yaml_io import JsonObject
    from dsctl.upstream.protocol import (
        ScheduleOperations,
        SchedulePayloadRecord,
    )

_ATTACHED_SCHEDULE_PAGE_SIZE = 2
_PROJECT_NOT_FOUND = 10018
_USER_NO_OPERATION_PERMISSION = 30001
_WORKFLOW_NOT_FOUND = 50003
AttachedScheduleLookupPhase: TypeAlias = Literal[
    "read",
    "pre_mutation",
    "post_mutation_refresh",
]


def load_attached_schedule(
    *,
    adapter: ScheduleOperations,
    project_code: int,
    workflow_code: int,
    workflow_name: str | None,
    action: str,
    phase: AttachedScheduleLookupPhase,
) -> SchedulePayloadRecord | None:
    """Load the zero-or-one schedule attached to one workflow."""
    mutation_applied = phase == "post_mutation_refresh"
    try:
        page = adapter.list(
            project_code=project_code,
            workflow_code=workflow_code,
            search=None,
            page_no=1,
            page_size=_ATTACHED_SCHEDULE_PAGE_SIZE,
        )
    except ApiResultError as error:
        _raise_attached_schedule_lookup_error(
            error,
            project_code=project_code,
            workflow_code=workflow_code,
            workflow_name=workflow_name,
            action=action,
            phase=phase,
            mutation_applied=mutation_applied,
        )
    except (ApiHttpError, ApiTransportError) as error:
        if not mutation_applied:
            raise
        _raise_attached_schedule_refresh_transport_error(
            error,
            project_code=project_code,
            workflow_code=workflow_code,
            workflow_name=workflow_name,
            action=action,
            phase=phase,
        )

    schedules = list(page.totalList or [])
    reported_total = page.total
    valid_total = isinstance(reported_total, int) and not isinstance(
        reported_total, bool
    )
    if valid_total and reported_total == 0 and not schedules:
        return None
    invalid_fields: list[str] = []
    if valid_total and reported_total == 1 and len(schedules) == 1:
        schedule = schedules[0]
        invalid_fields = _attached_schedule_invalid_fields(
            schedule,
            workflow_code=workflow_code,
        )
        if not invalid_fields:
            return schedule

    message = (
        "DolphinScheduler returned inconsistent attached-schedule state for "
        "the selected workflow"
    )
    raise ApiTransportError(
        message,
        details={
            "resource": WORKFLOW_RESOURCE,
            "dependency_resource": SCHEDULE_RESOURCE,
            "project_code": project_code,
            "workflow_code": workflow_code,
            "operation": action,
            "phase": phase,
            "mutation_applied": mutation_applied,
            "reported_total": reported_total,
            "returned_count": len(schedules),
            "schedule_ids": [schedule.id for schedule in schedules],
            "returned_workflow_codes": [
                schedule.workflowDefinitionCode for schedule in schedules
            ],
            "invalid_fields": invalid_fields,
        },
        suggestion=_attached_schedule_suggestion(
            project_code=project_code,
            workflow_code=workflow_code,
            mutation_applied=mutation_applied,
        ),
    )


def _raise_attached_schedule_lookup_error(
    error: ApiResultError,
    *,
    project_code: int,
    workflow_code: int,
    workflow_name: str | None,
    action: str,
    phase: AttachedScheduleLookupPhase,
    mutation_applied: bool,
) -> NoReturn:
    details: JsonObject = {
        "resource": WORKFLOW_RESOURCE,
        "dependency_resource": SCHEDULE_RESOURCE,
        "operation": action,
        "phase": phase,
        "project_code": project_code,
        "workflow_code": workflow_code,
        "mutation_applied": mutation_applied,
        "upstream_result_code": error.result_code,
        "upstream_result_message": error.result_message,
    }
    if workflow_name is not None:
        details["workflow_name"] = workflow_name
    suggestion = _attached_schedule_suggestion(
        project_code=project_code,
        workflow_code=workflow_code,
        mutation_applied=mutation_applied,
    )
    if error.result_code == _PROJECT_NOT_FOUND:
        message = (
            f"Project code {project_code} was not found while loading workflow state."
        )
        raise NotFoundError(
            message,
            details=details,
            suggestion=suggestion,
        ) from error
    if error.result_code == _WORKFLOW_NOT_FOUND:
        message = (
            f"Workflow code {workflow_code} was not found while loading its schedule."
        )
        raise NotFoundError(
            message,
            details=details,
            suggestion=suggestion,
        ) from error
    if error.result_code == _USER_NO_OPERATION_PERMISSION:
        message = (
            "Loading the workflow's attached schedule requires project permission."
        )
        permission_suggestion = (
            "Ask a DolphinScheduler administrator for the project permission needed "
            "to inspect schedules in this project. "
        )
        if mutation_applied:
            permission_suggestion += (
                "The workflow mutation completed; do not retry it until permission "
                "is granted and you have inspected the workflow and schedule."
            )
        else:
            permission_suggestion += "Then retry the workflow command."
        raise PermissionDeniedError(
            message,
            details=details,
            suggestion=permission_suggestion,
        ) from error
    message = "DolphinScheduler could not load the workflow's attached schedule."
    raise ApiTransportError(
        message,
        details=details,
        suggestion=suggestion,
    ) from error


def _attached_schedule_suggestion(
    *,
    project_code: int,
    workflow_code: int,
    mutation_applied: bool,
) -> str:
    verification = (
        f"`dsctl schedule list --project {project_code} --workflow {workflow_code}`"
    )
    if mutation_applied:
        return (
            f"The workflow mutation completed. Do not retry it before running "
            f"{verification} and `dsctl workflow get {workflow_code} --project "
            f"{project_code}` to verify current state."
        )
    return f"Run {verification} to verify the attached schedule, then retry."


def _raise_attached_schedule_refresh_transport_error(
    error: ApiHttpError | ApiTransportError,
    *,
    project_code: int,
    workflow_code: int,
    workflow_name: str | None,
    action: str,
    phase: AttachedScheduleLookupPhase,
) -> NoReturn:
    details: JsonObject = {
        "resource": WORKFLOW_RESOURCE,
        "dependency_resource": SCHEDULE_RESOURCE,
        "operation": action,
        "phase": phase,
        "project_code": project_code,
        "workflow_code": workflow_code,
        "mutation_applied": True,
        "upstream_error_type": error.error_type,
        "upstream_error_details": error.details,
    }
    if workflow_name is not None:
        details["workflow_name"] = workflow_name
    suggestion = _attached_schedule_suggestion(
        project_code=project_code,
        workflow_code=workflow_code,
        mutation_applied=True,
    )
    message = (
        "The workflow mutation completed, but the CLI could not refresh its "
        "attached schedule."
    )
    if isinstance(error, ApiHttpError):
        raise ApiHttpError(
            message,
            status_code=error.status_code,
            body=error.body,
            details=details,
            suggestion=suggestion,
        ) from error
    raise ApiTransportError(
        message,
        details=details,
        suggestion=suggestion,
    ) from error


def _attached_schedule_invalid_fields(
    schedule: SchedulePayloadRecord,
    *,
    workflow_code: int,
) -> list[str]:
    invalid_fields: list[str] = []
    if schedule.workflowDefinitionCode != workflow_code:
        invalid_fields.append("workflowDefinitionCode")
    if (
        not isinstance(schedule.id, int)
        or isinstance(schedule.id, bool)
        or schedule.id <= 0
    ):
        invalid_fields.append("id")
    for field_name, value in (
        ("crontab", schedule.crontab),
        ("timezoneId", schedule.timezoneId),
        ("startTime", schedule.startTime),
        ("endTime", schedule.endTime),
    ):
        if not isinstance(value, str) or not value.strip():
            invalid_fields.append(field_name)
    if enum_value(schedule.releaseState) not in {"ONLINE", "OFFLINE"}:
        invalid_fields.append("releaseState")
    return invalid_fields
