from __future__ import annotations

from typing import TYPE_CHECKING, Literal, TypedDict

from dsctl.cli_surface import SCHEDULE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ConflictError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.services._serialization import optional_text
from dsctl.services._surface_metadata import CONFIRMATION_RETRY_OPTION
from dsctl.services._validation import (
    require_non_empty_text,
    require_non_negative_int,
    require_quartz_cron_text,
)
from dsctl.services.confirmation import build_confirmation_token, require_confirmation
from dsctl.services.schedule_analysis import (
    RiskLevel,
    SchedulePreviewData,
    build_schedule_preview_data,
    confirmed_high_frequency_warning,
)

if TYPE_CHECKING:
    from dsctl.services.runtime import ServiceRuntime
    from dsctl.upstream.protocol import SchedulePayloadRecord

FailureStrategyValue = str
WarningTypeValue = str
PriorityValue = str

FAILURE_STRATEGIES = frozenset({"CONTINUE", "END"})
WARNING_TYPES = frozenset({"NONE", "SUCCESS", "FAILURE", "ALL"})
PRIORITIES = frozenset({"HIGHEST", "HIGH", "MEDIUM", "LOW", "LOWEST"})

TENANT_NOT_EXIST = 10017
SCHEDULE_CRON_ONLINE_FORBID_UPDATE = 10023
SCHEDULE_CRON_CHECK_FAILED = 10024
SCHEDULE_START_TIME_END_TIME_SAME = 10141
SCHEDULE_NOT_EXISTS = 10203
SCHEDULE_ALREADY_EXISTS = 10204
WORKFLOW_DEFINITION_NOT_EXIST = 50003
WORKFLOW_DEFINITION_NOT_RELEASE = 50004
SCHEDULE_STATE_ONLINE = 50023
START_TIME_BIGGER_THAN_END_TIME_ERROR = 80003
USER_NO_OPERATION_PERM = 30001


class ScheduleCreateInput(TypedDict):
    """Validated schedule create/update payload shared across services."""

    crontab: str
    start_time: str
    end_time: str
    timezone_id: str
    failure_strategy: str | None
    warning_type: str | None
    warning_group_id: int
    workflow_instance_priority: str | None
    worker_group: str | None
    tenant_code: str | None
    environment_code: int


class ScheduleCreateDraft(TypedDict):
    """Validated create-draft input before runtime default resolution."""

    crontab: str
    start_time: str
    end_time: str
    timezone_id: str
    failure_strategy: str | None
    warning_type: str | None
    warning_group_id: int | None
    workflow_instance_priority: str | None
    worker_group: str | None
    tenant_code: str | None
    environment_code: int | None


class SchedulePreviewInput(TypedDict):
    """Validated input subset needed by schedule preview and risk analysis."""

    crontab: str
    start_time: str
    end_time: str
    timezone_id: str


class ScheduleMutationData(TypedDict):
    """Normalized schedule mutation payload emitted by explain surfaces."""

    crontab: str
    startTime: str
    endTime: str
    timezoneId: str
    failureStrategy: str | None
    warningType: str | None
    warningGroupId: int
    workflowInstancePriority: str | None
    workerGroup: str | None
    tenantCode: str | None
    environmentCode: int


ScheduleExplainNextAction = Literal["apply", "retry_with_confirmation"]


class ScheduleConfirmationData(TypedDict):
    """Machine-readable confirmation state derived from schedule risk analysis."""

    required: bool
    nextAction: ScheduleExplainNextAction
    token: str | None
    confirmFlag: str | None
    retryOption: str
    riskType: str | None
    riskLevel: RiskLevel
    reason: str | None


class ScheduleWarningDetail(TypedDict):
    """Structured warning emitted after one confirmed high-frequency schedule."""

    code: str
    message: str
    risk_type: str | None
    min_interval_seconds: int | None
    threshold_seconds: int


def validated_schedule_preview_input(
    *,
    cron: str | None,
    start: str | None,
    end: str | None,
    timezone: str | None,
) -> SchedulePreviewInput:
    """Validate one schedule preview input set."""
    if cron is None or start is None or end is None or timezone is None:
        message = "schedule preview requires --cron, --start, --end, and --timezone"
        raise UserInputError(
            message,
            suggestion=(
                "Pass all four schedule fields for an ad hoc preview, or provide "
                "an existing schedule id to preview its current fire times."
            ),
        )
    return SchedulePreviewInput(
        crontab=require_quartz_cron_text(cron, label="cron"),
        start_time=require_non_empty_text(start, label="start"),
        end_time=require_non_empty_text(end, label="end"),
        timezone_id=require_non_empty_text(timezone, label="timezone"),
    )


def schedule_preview_input_from_payload(
    schedule: SchedulePayloadRecord,
) -> SchedulePreviewInput:
    """Extract one preview input from an existing schedule payload."""
    return SchedulePreviewInput(
        crontab=required_update_text(
            None,
            fallback=schedule.crontab,
            label="cron",
        ),
        start_time=required_update_text(
            None,
            fallback=schedule.startTime,
            label="start",
        ),
        end_time=required_update_text(
            None,
            fallback=schedule.endTime,
            label="end",
        ),
        timezone_id=required_update_text(
            None,
            fallback=schedule.timezoneId,
            label="timezone",
        ),
    )


def validated_schedule_create_input(
    *,
    cron: str,
    start: str,
    end: str,
    timezone: str,
    failure_strategy: str | None,
    warning_type: str | None,
    warning_group_id: int,
    priority: str | None,
    worker_group: str | None,
    tenant_code: str | None,
    environment_code: int,
) -> ScheduleCreateInput:
    """Validate one schedule create/update payload."""
    return {
        "crontab": require_quartz_cron_text(cron, label="cron"),
        "start_time": require_non_empty_text(start, label="start"),
        "end_time": require_non_empty_text(end, label="end"),
        "timezone_id": require_non_empty_text(timezone, label="timezone"),
        "failure_strategy": validated_optional_enum(
            failure_strategy,
            allowed=FAILURE_STRATEGIES,
            label="failure_strategy",
        ),
        "warning_type": validated_optional_enum(
            warning_type,
            allowed=WARNING_TYPES,
            label="warning_type",
        ),
        "warning_group_id": require_non_negative_int(
            warning_group_id,
            label="warning_group_id",
        ),
        "workflow_instance_priority": validated_optional_enum(
            priority,
            allowed=PRIORITIES,
            label="priority",
        ),
        "worker_group": optional_text(worker_group),
        "tenant_code": optional_text(tenant_code),
        "environment_code": require_non_negative_int(
            environment_code,
            label="environment_code",
        ),
    }


def validated_schedule_create_draft(
    *,
    cron: str,
    start: str,
    end: str,
    timezone: str,
    failure_strategy: str | None,
    warning_type: str | None,
    warning_group_id: int | None,
    priority: str | None,
    worker_group: str | None,
    tenant_code: str | None,
    environment_code: int | None,
) -> ScheduleCreateDraft:
    """Validate one schedule create payload before runtime defaults apply."""
    return {
        "crontab": require_quartz_cron_text(cron, label="cron"),
        "start_time": require_non_empty_text(start, label="start"),
        "end_time": require_non_empty_text(end, label="end"),
        "timezone_id": require_non_empty_text(timezone, label="timezone"),
        "failure_strategy": validated_optional_enum(
            failure_strategy,
            allowed=FAILURE_STRATEGIES,
            label="failure_strategy",
        ),
        "warning_type": validated_optional_enum(
            warning_type,
            allowed=WARNING_TYPES,
            label="warning_type",
        ),
        "warning_group_id": (
            None
            if warning_group_id is None
            else require_non_negative_int(
                warning_group_id,
                label="warning_group_id",
            )
        ),
        "workflow_instance_priority": validated_optional_enum(
            priority,
            allowed=PRIORITIES,
            label="priority",
        ),
        "worker_group": optional_text(worker_group),
        "tenant_code": optional_text(tenant_code),
        "environment_code": (
            None
            if environment_code is None
            else require_non_negative_int(
                environment_code,
                label="environment_code",
            )
        ),
    }


def preview_schedule(
    runtime: ServiceRuntime,
    *,
    project_code: int,
    schedule_input: SchedulePreviewInput,
) -> SchedulePreviewData:
    """Preview schedule fire times and return one structured analysis payload."""
    try:
        preview_times = runtime.upstream.schedules.preview(
            project_code=project_code,
            crontab=schedule_input["crontab"],
            start_time=schedule_input["start_time"],
            end_time=schedule_input["end_time"],
            timezone_id=schedule_input["timezone_id"],
        )
    except ApiResultError as error:
        raise translate_schedule_api_error(
            error,
            operation="preview",
        ) from error
    return build_schedule_preview_data(list(preview_times))


def schedule_mutation_data(
    *,
    crontab: str,
    start_time: str,
    end_time: str,
    timezone_id: str,
    failure_strategy: str | None,
    warning_type: str | None,
    warning_group_id: int,
    workflow_instance_priority: str | None,
    worker_group: str | None,
    tenant_code: str | None,
    environment_code: int,
) -> ScheduleMutationData:
    """Render one normalized schedule mutation shape with DS-native field names."""
    return {
        "crontab": crontab,
        "startTime": start_time,
        "endTime": end_time,
        "timezoneId": timezone_id,
        "failureStrategy": failure_strategy,
        "warningType": warning_type,
        "warningGroupId": warning_group_id,
        "workflowInstancePriority": workflow_instance_priority,
        "workerGroup": worker_group,
        "tenantCode": tenant_code,
        "environmentCode": environment_code,
    }


def schedule_confirmation_data(
    *,
    action: str,
    preview: SchedulePreviewData,
    schedule_payload: dict[str, object],
) -> ScheduleConfirmationData:
    """Build stable confirmation metadata for one schedule mutation."""
    analysis = preview["analysis"]
    required = analysis["requires_confirmation"]
    token = (
        build_confirmation_token(
            action=action,
            payload=_schedule_confirmation_payload(
                preview=preview,
                schedule_payload=schedule_payload,
            ),
        )
        if required
        else None
    )
    confirm_flag = None if token is None else f"{CONFIRMATION_RETRY_OPTION} {token}"
    return {
        "required": required,
        "nextAction": ("retry_with_confirmation" if required else "apply"),
        "token": token,
        "confirmFlag": confirm_flag,
        "retryOption": CONFIRMATION_RETRY_OPTION,
        "riskType": analysis["risk_type"],
        "riskLevel": analysis["risk_level"],
        "reason": analysis["reason"],
    }


def require_high_frequency_confirmation(
    *,
    action: str,
    confirmation: str | None,
    preview: SchedulePreviewData,
    schedule_payload: dict[str, object],
) -> None:
    """Require explicit confirmation for one high-frequency schedule mutation."""
    confirmation_data = schedule_confirmation_data(
        action=action,
        preview=preview,
        schedule_payload=schedule_payload,
    )
    if not confirmation_data["required"]:
        return
    require_confirmation(
        action=action,
        confirmation=confirmation,
        payload=_schedule_confirmation_payload(
            preview=preview,
            schedule_payload=schedule_payload,
        ),
        message=(
            "This schedule triggers more frequently than every 10 minutes and "
            "requires explicit confirmation."
        ),
        details={
            "risk_type": confirmation_data["riskType"],
            "risk_level": confirmation_data["riskLevel"],
            "preview": preview,
        },
    )


def confirmed_preview_warnings(preview: SchedulePreviewData) -> list[str]:
    """Render post-confirmation warnings derived from preview analysis."""
    warning = confirmed_high_frequency_warning(preview)
    if warning is None:
        return []
    return [warning]


def confirmed_preview_warning_details(
    preview: SchedulePreviewData,
) -> list[ScheduleWarningDetail]:
    """Return structured warning details for confirmed preview-derived risks."""
    warning = confirmed_high_frequency_warning(preview)
    if warning is None:
        return []
    analysis = preview["analysis"]
    return [
        {
            "code": "confirmed_high_frequency_schedule",
            "message": warning,
            "risk_type": analysis["risk_type"],
            "min_interval_seconds": analysis["min_interval_seconds"],
            "threshold_seconds": analysis["threshold_seconds"],
        }
    ]


def translate_schedule_api_error(
    error: ApiResultError,
    *,
    operation: str,
    schedule_id: int | None = None,
    workflow_code: int | None = None,
    workflow_name: str | None = None,
) -> Exception:
    """Translate one upstream schedule API error into a stable CLI error."""
    details: dict[str, int | str] = {
        "resource": SCHEDULE_RESOURCE,
        "operation": operation,
    }
    if schedule_id is not None:
        details["schedule_id"] = schedule_id
    if workflow_code is not None:
        details["workflow_code"] = workflow_code
    if workflow_name is not None:
        details["workflow_name"] = workflow_name

    translated = _translated_schedule_missing_or_conflict_error(
        error,
        details=details,
        schedule_id=schedule_id,
    )
    if translated is not None:
        return translated
    translated = _translated_schedule_input_or_state_error(error, details=details)
    if translated is not None:
        return translated
    if error.result_code == USER_NO_OPERATION_PERM:
        message = f"Schedule {operation} requires additional permissions"
        return PermissionDeniedError(message, details=details)
    return error


def _translated_schedule_missing_or_conflict_error(
    error: ApiResultError,
    *,
    details: dict[str, int | str],
    schedule_id: int | None,
) -> Exception | None:
    if error.result_code == SCHEDULE_NOT_EXISTS:
        target = f"Schedule {schedule_id}" if schedule_id is not None else "Schedule"
        return NotFoundError(f"{target} does not exist.", details=details)
    if error.result_code == WORKFLOW_DEFINITION_NOT_EXIST:
        return NotFoundError(
            "The workflow bound to this schedule does not exist.",
            details=details,
        )
    if error.result_code == SCHEDULE_ALREADY_EXISTS:
        return ConflictError(
            "This workflow already has a schedule. Use schedule update or delete "
            "the existing schedule first.",
            details=details,
        )
    return None


def _translated_schedule_input_or_state_error(
    error: ApiResultError,
    *,
    details: dict[str, int | str],
) -> Exception | None:
    if error.result_code in {
        TENANT_NOT_EXIST,
        SCHEDULE_CRON_CHECK_FAILED,
        SCHEDULE_START_TIME_END_TIME_SAME,
        START_TIME_BIGGER_THAN_END_TIME_ERROR,
    }:
        return UserInputError(
            error.result_message,
            details=details,
            suggestion=(
                "Run `dsctl schedule explain` with the same flags to validate "
                "the mutation before retrying."
            ),
        )
    if error.result_code == WORKFLOW_DEFINITION_NOT_RELEASE:
        return InvalidStateError(
            "The workflow must be online before this schedule operation can proceed.",
            details=details,
            suggestion=(
                "Bring the owning workflow online, then retry the schedule operation."
            ),
        )
    if error.result_code == SCHEDULE_CRON_ONLINE_FORBID_UPDATE:
        return InvalidStateError(
            "Online schedules cannot be updated. Bring the schedule offline "
            "before updating it.",
            details=details,
            suggestion=_schedule_offline_retry_suggestion(
                details=details,
                operation="update",
            ),
        )
    if error.result_code == SCHEDULE_STATE_ONLINE:
        return InvalidStateError(
            "Online schedules cannot be deleted. Bring the schedule offline "
            "before deleting it.",
            details=details,
            suggestion=_schedule_offline_retry_suggestion(
                details=details,
                operation="delete",
            ),
        )
    return None


def required_update_text(
    value: str | None,
    *,
    fallback: str | None,
    label: str,
) -> str:
    """Resolve one required update field from explicit input or remote fallback."""
    if value is not None:
        return require_non_empty_text(value, label=label)
    if fallback is None:
        message = f"Schedule payload was missing required field {label!r}"
        raise UserInputError(
            message,
            suggestion=(
                "Inspect the current schedule with `dsctl schedule get SCHEDULE_ID` "
                "before retrying the update."
            ),
        )
    return require_non_empty_text(fallback, label=label)


def validated_optional_enum(
    value: str | None,
    *,
    allowed: frozenset[str],
    label: str,
) -> str | None:
    """Normalize one optional enum string to its canonical uppercase value."""
    normalized = optional_text(value)
    if normalized is None:
        return None
    normalized_upper = normalized.upper()
    if normalized_upper not in allowed:
        message = f"{label} must be one of: {', '.join(sorted(allowed))}"
        raise UserInputError(
            message,
            suggestion=f"Pass one of: {', '.join(sorted(allowed))}.",
        )
    return normalized_upper


def _schedule_offline_retry_suggestion(
    *,
    details: dict[str, int | str],
    operation: str,
) -> str:
    schedule_id = details.get("schedule_id")
    if isinstance(schedule_id, int):
        return (
            f"Run `dsctl schedule offline {schedule_id}`, then retry "
            f"`schedule {operation}`."
        )
    return (
        "Bring the schedule offline first, then retry the same schedule "
        f"{operation} operation."
    )


def updated_optional_enum(
    value: str | None,
    *,
    current: str | None,
    allowed: frozenset[str],
    label: str,
) -> str | None:
    """Resolve one optional enum update against the current remote value."""
    if value is None:
        return current
    return validated_optional_enum(value, allowed=allowed, label=label)


def _schedule_confirmation_payload(
    *,
    preview: SchedulePreviewData,
    schedule_payload: dict[str, object],
) -> dict[str, object]:
    return {
        "preview": preview,
        "schedule": schedule_payload,
    }
