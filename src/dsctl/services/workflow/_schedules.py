from __future__ import annotations

from typing import TYPE_CHECKING, NoReturn, cast

from dsctl.cli_surface import (
    SCHEDULE_RESOURCE,
    WORKFLOW_RESOURCE,
)
from dsctl.errors import (
    ApiHttpError,
    ApiResultError,
    ApiTransportError,
    NotFoundError,
    PermissionDeniedError,
)
from dsctl.services._workflow.schedule import raise_attached_schedule_lookup_error
from dsctl.upstream.definition_models import (
    NativeId,
    ScheduleView,
    WorkflowScope,
    schedule_has_missed_fire_policy,
)
from dsctl.upstream.protocols.design import ScheduleMissedFireRecord
from dsctl.upstream.serialization import (
    enum_value,
)

if TYPE_CHECKING:
    from dsctl.services._workflow.schedule import AttachedScheduleLookupPhase
    from dsctl.support.yaml_io import JsonObject
    from dsctl.upstream.protocol import (
        ScheduleRecord,
    )

from dsctl.services.workflow._selection import (
    _identity_field,
    _native_code,
    _ResolvedWorkflowTarget,
)
from dsctl.services.workflow._types import (
    _PROJECT_NOT_EXIST,
    _USER_NO_OPERATION_PERMISSION,
    _USER_NO_OPERATION_PROJECT_PERMISSION,
    PROJECT_NOT_FOUND,
    WORKFLOW_DEFINITION_NOT_EXIST,
    WorkflowServiceRuntime,
)


def _schedule_view(
    scope: WorkflowScope,
    schedule: ScheduleRecord | None,
) -> ScheduleView | None:
    if schedule is None:
        return None
    return ScheduleView(
        id=schedule.id,
        workflow_native=scope.workflow.native,
        workflow_name=scope.workflow.name,
        project_name=scope.project.name,
        start_time=schedule.startTime,
        end_time=schedule.endTime,
        timezone_id=schedule.timezoneId,
        crontab=schedule.crontab,
        failure_strategy=enum_value(schedule.failureStrategy),
        workflow_instance_priority=enum_value(schedule.workflowInstancePriority),
        release_state=enum_value(schedule.releaseState),
        has_missed_fire_policy=schedule_has_missed_fire_policy(schedule),
        missed_fire_policy=(
            enum_value(schedule.missedFirePolicy)
            if isinstance(schedule, ScheduleMissedFireRecord)
            else None
        ),
    )


def _workflow_scope_data(
    scope: WorkflowScope,
    attached_schedule: ScheduleRecord | None,
) -> JsonObject:
    return scope.view.to_data(
        attached_schedule=_schedule_view(scope, attached_schedule)
    )


def _load_target_attached_schedule(
    runtime: WorkflowServiceRuntime,
    *,
    target: _ResolvedWorkflowTarget,
    action: str,
    phase: AttachedScheduleLookupPhase,
) -> ScheduleRecord | None:
    try:
        return cast(
            "ScheduleRecord | None",
            runtime.domain.schedules.attached(target.scope),
        )
    except ApiResultError as error:
        if isinstance(target.project.native, NativeId) and isinstance(
            target.workflow.native, NativeId
        ):
            _raise_legacy_attached_schedule_lookup_error(
                error,
                target=target,
                action=action,
                phase=phase,
            )
        raise_attached_schedule_lookup_error(
            error,
            project_code=_native_code(target.project, label="project"),
            workflow_code=_native_code(target.workflow, label="workflow"),
            workflow_name=target.workflow.name,
            action=action,
            phase=phase,
            mutation_applied=phase == "post_mutation_refresh",
            schedule_list_supported=True,
        )
    except (
        ApiHttpError,
        ApiTransportError,
        NotFoundError,
        PermissionDeniedError,
    ) as error:
        if phase != "post_mutation_refresh":
            raise
        _raise_attached_schedule_refresh_error(
            error,
            target=target,
            action=action,
            phase=phase,
        )


def _raise_attached_schedule_refresh_error(
    error: ApiHttpError | ApiTransportError | NotFoundError | PermissionDeniedError,
    *,
    target: _ResolvedWorkflowTarget,
    action: str,
    phase: AttachedScheduleLookupPhase,
) -> NoReturn:
    details = {
        **error.details,
        "resource": WORKFLOW_RESOURCE,
        "dependency_resource": SCHEDULE_RESOURCE,
        "operation": action,
        "phase": phase,
        "mutation_applied": True,
        _identity_field(target.project, prefix="project_"): (
            target.project.native.value
        ),
        _identity_field(target.workflow, prefix="workflow_"): (
            target.workflow.native.value
        ),
        "upstream_error_type": error.error_type,
    }
    suggestion = (
        "The workflow mutation completed; do not retry it until you inspect the "
        "workflow and its attached schedule."
    )
    message = (
        "The workflow mutation completed, but the CLI could not refresh its "
        "attached schedule."
    )
    if isinstance(error, PermissionDeniedError):
        raise PermissionDeniedError(
            message,
            details=details,
            source=error.source,
            suggestion=suggestion,
        ) from error
    if isinstance(error, NotFoundError):
        raise NotFoundError(
            message,
            details=details,
            source=error.source,
            suggestion=suggestion,
        ) from error
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
        source=error.source,
        suggestion=suggestion,
    ) from error


def _raise_legacy_attached_schedule_lookup_error(
    error: ApiResultError,
    *,
    target: _ResolvedWorkflowTarget,
    action: str,
    phase: AttachedScheduleLookupPhase,
) -> NoReturn:
    """Translate id-native schedule lookup failures without inventing codes."""
    details: JsonObject = {
        "resource": WORKFLOW_RESOURCE,
        "dependency_resource": SCHEDULE_RESOURCE,
        "operation": action,
        "phase": phase,
        "mutation_applied": phase == "post_mutation_refresh",
        "project_id": target.project.native.value,
        "workflow_id": target.workflow.native.value,
        "upstream_result_code": error.result_code,
        "upstream_result_message": error.result_message,
    }
    if target.project.name is not None:
        details["project_name"] = target.project.name
    if target.workflow.name is not None:
        details["workflow_name"] = target.workflow.name
    suggestion = (
        "Inspect the workflow and its attached schedule by name, then retry only "
        "after the selected project state is consistent."
    )
    if error.result_code in {PROJECT_NOT_FOUND, _PROJECT_NOT_EXIST}:
        message = "The selected project was not found while loading workflow state."
        raise NotFoundError(
            message,
            details=details,
            suggestion=suggestion,
        ) from error
    if error.result_code == WORKFLOW_DEFINITION_NOT_EXIST:
        message = "The selected workflow was not found while loading its schedule."
        raise NotFoundError(
            message,
            details=details,
            suggestion=suggestion,
        ) from error
    if error.result_code in {
        _USER_NO_OPERATION_PERMISSION,
        _USER_NO_OPERATION_PROJECT_PERMISSION,
    }:
        message = (
            "Loading the workflow's attached schedule requires project permission."
        )
        raise PermissionDeniedError(
            message,
            details=details,
            suggestion=suggestion,
        ) from error
    message = "DolphinScheduler could not load the workflow's attached schedule."
    raise ApiTransportError(
        message,
        details=details,
        suggestion=suggestion,
    ) from error
