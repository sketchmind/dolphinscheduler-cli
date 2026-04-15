from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, NotRequired, TypeAlias, TypedDict, cast

from dsctl.cli_surface import SCHEDULE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    UserInputError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._runtime_defaults import (
    ProjectPreferenceDefaults,
    load_project_preference_defaults,
    select_tenant_code,
)
from dsctl.services._schedule_support import (
    FailureStrategyValue,
    PriorityValue,
    ScheduleConfirmationData,
    ScheduleCreateDraft,
    ScheduleCreateInput,
    ScheduleMutationData,
    SchedulePreviewInput,
    WarningTypeValue,
    confirmed_preview_warning_details,
    confirmed_preview_warnings,
    preview_schedule,
    require_high_frequency_confirmation,
    required_update_text,
    schedule_confirmation_data,
    schedule_mutation_data,
    schedule_preview_input_from_payload,
    translate_schedule_api_error,
    updated_optional_enum,
    validated_schedule_create_draft,
    validated_schedule_create_input,
    validated_schedule_preview_input,
)
from dsctl.services._serialization import (
    ScheduleData,
    enum_value,
    optional_text,
    serialize_schedule,
)
from dsctl.services._validation import (
    require_delete_force,
    require_non_negative_int,
    require_positive_int,
    require_quartz_cron_text,
)
from dsctl.services.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_AUTO_EXHAUST_PAGES,
    PageData,
    requested_page_data,
)
from dsctl.services.resolver import (
    ResolvedProject,
    ResolvedWorkflow,
)
from dsctl.services.resolver import project as resolve_project
from dsctl.services.resolver import workflow as resolve_workflow
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime
from dsctl.services.selection import (
    SelectedValue,
    require_project_selection,
    require_workflow_selection,
    selected_value_data,
    with_selection_source,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.services.schedule_analysis import SchedulePreviewData
    from dsctl.services.selection import SelectionData
    from dsctl.support.yaml_io import JsonObject
    from dsctl.upstream.protocol import SchedulePayloadRecord


SchedulePageData: TypeAlias = PageData[ScheduleData]
ScheduleExplainField: TypeAlias = Literal[
    "crontab",
    "startTime",
    "endTime",
    "timezoneId",
    "failureStrategy",
    "warningType",
    "warningGroupId",
    "workflowInstancePriority",
    "workerGroup",
    "tenantCode",
    "environmentCode",
]
ScheduleExplainFieldValue: TypeAlias = str | int | None
ScheduleExplainFieldGroups: TypeAlias = tuple[
    list[ScheduleExplainField],
    list[ScheduleExplainField],
    list[ScheduleExplainField],
]

_SCHEDULE_EXPLAIN_FIELDS: tuple[ScheduleExplainField, ...] = (
    "crontab",
    "startTime",
    "endTime",
    "timezoneId",
    "failureStrategy",
    "warningType",
    "warningGroupId",
    "workflowInstancePriority",
    "workerGroup",
    "tenantCode",
    "environmentCode",
)


@dataclass(frozen=True)
class _EffectiveScheduleCreateInput:
    """Create-mutation input after runtime default resolution."""

    schedule_input: ScheduleCreateInput
    tenant_selection: SelectedValue
    project_preference_used_fields: tuple[str, ...]


@dataclass(frozen=True)
class _ProjectPreferenceScheduleOverrides:
    """Project-preference values applied to schedule create/explain."""

    warning_type: str | None
    warning_group_id: int | None
    workflow_instance_priority: str | None
    worker_group: str | None
    environment_code: int | None
    used_fields: tuple[str, ...]


class DeleteScheduleData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    schedule: ScheduleData


class ExplainScheduleData(TypedDict):
    """Explain one schedule mutation before execution."""

    mutationAction: str
    proposedSchedule: ScheduleMutationData
    currentSchedule: NotRequired[ScheduleMutationData]
    requestedFields: NotRequired[list[str]]
    changedFields: NotRequired[list[str]]
    inheritedFields: NotRequired[list[str]]
    unchangedRequestedFields: NotRequired[list[str]]
    preview: SchedulePreviewData
    confirmation: ScheduleConfirmationData


def list_schedules_result(
    *,
    project: str | None = None,
    workflow: str | None = None,
    search: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """List schedules inside one resolved project."""
    require_positive_int(page_no, label="page_no")
    require_positive_int(page_size, label="page_size")
    normalized_search = optional_text(search)
    normalized_workflow = optional_text(workflow)
    if normalized_search is not None and normalized_workflow is not None:
        message = "--workflow and --search cannot be used together"
        raise UserInputError(
            message,
            suggestion=(
                "Use `--workflow` to scope to one workflow, or drop it and use "
                "`--search` across schedules in the selected project."
            ),
        )

    return run_with_service_runtime(
        env_file,
        _list_schedules_result,
        project=project,
        workflow=normalized_workflow,
        search=normalized_search,
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
    )


def get_schedule_result(
    schedule_id: int,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Get one schedule by id."""
    require_positive_int(schedule_id, label="schedule id")
    return run_with_service_runtime(
        env_file,
        _get_schedule_result,
        schedule_id=schedule_id,
    )


def preview_schedule_result(
    *,
    schedule_id: int | None = None,
    project: str | None = None,
    cron: str | None = None,
    start: str | None = None,
    end: str | None = None,
    timezone: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Preview the next fire times for an existing or proposed schedule."""
    if schedule_id is not None:
        require_positive_int(schedule_id, label="schedule id")
        if any(value is not None for value in (project, cron, start, end, timezone)):
            message = "schedule id preview does not accept project or schedule fields"
            raise UserInputError(
                message,
                suggestion=(
                    "Pass only the schedule id to preview an existing schedule, "
                    "or omit the id and pass `--project`, `--cron`, `--start`, "
                    "`--end`, and `--timezone` for an ad hoc preview."
                ),
            )
        return run_with_service_runtime(
            env_file,
            _preview_schedule_result,
            schedule_id=schedule_id,
        )

    schedule_input = validated_schedule_preview_input(
        cron=cron,
        start=start,
        end=end,
        timezone=timezone,
    )
    return run_with_service_runtime(
        env_file,
        _preview_ad_hoc_schedule_result,
        project=project,
        schedule_input=schedule_input,
    )


def explain_schedule_result(
    *,
    schedule_id: int | None = None,
    workflow: str | None = None,
    project: str | None = None,
    cron: str | None = None,
    start: str | None = None,
    end: str | None = None,
    timezone: str | None = None,
    failure_strategy: FailureStrategyValue | None = None,
    warning_type: WarningTypeValue | None = None,
    warning_group_id: int | None = None,
    priority: PriorityValue | None = None,
    worker_group: str | None = None,
    tenant_code: str | None = None,
    environment_code: int | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Explain one schedule create or update mutation without changing remote state."""
    if schedule_id is None:
        schedule_input = validated_schedule_create_draft(
            cron=_required_text_option(cron, label="cron"),
            start=_required_text_option(start, label="start"),
            end=_required_text_option(end, label="end"),
            timezone=_required_text_option(timezone, label="timezone"),
            failure_strategy=failure_strategy,
            warning_type=warning_type,
            warning_group_id=warning_group_id,
            priority=priority,
            worker_group=worker_group,
            tenant_code=tenant_code,
            environment_code=environment_code,
        )
        return run_with_service_runtime(
            env_file,
            _explain_schedule_create_result,
            workflow=workflow,
            project=project,
            schedule_input=schedule_input,
        )

    require_positive_int(schedule_id, label="schedule id")
    if workflow is not None or project is not None or tenant_code is not None:
        message = (
            "schedule explain by id does not accept --workflow, --project, "
            "or --tenant-code"
        )
        raise UserInputError(
            message,
            suggestion=(
                "When explaining an existing schedule by id, pass only update "
                "flags. Omit the id if you want create-style explain with "
                "`--workflow` and schedule fields."
            ),
        )
    if (
        cron is None
        and start is None
        and end is None
        and timezone is None
        and failure_strategy is None
        and warning_type is None
        and warning_group_id is None
        and priority is None
        and worker_group is None
        and environment_code is None
    ):
        message = "Schedule explain by id requires at least one field change"
        raise UserInputError(
            message,
            suggestion=(
                "Pass at least one update flag such as --cron, --start, or --timezone."
            ),
        )
    return run_with_service_runtime(
        env_file,
        _explain_schedule_update_result,
        schedule_id=schedule_id,
        cron=cron,
        start=start,
        end=end,
        timezone=timezone,
        failure_strategy=failure_strategy,
        warning_type=warning_type,
        warning_group_id=warning_group_id,
        priority=priority,
        worker_group=worker_group,
        environment_code=environment_code,
    )


def create_schedule_result(
    *,
    workflow: str | None,
    project: str | None = None,
    cron: str,
    start: str,
    end: str,
    timezone: str,
    failure_strategy: FailureStrategyValue | None = None,
    warning_type: WarningTypeValue | None = None,
    warning_group_id: int | None = None,
    priority: PriorityValue | None = None,
    worker_group: str | None = None,
    tenant_code: str | None = None,
    environment_code: int | None = None,
    confirm_risk: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create one schedule bound to a resolved workflow."""
    schedule_input = validated_schedule_create_draft(
        cron=cron,
        start=start,
        end=end,
        timezone=timezone,
        failure_strategy=failure_strategy,
        warning_type=warning_type,
        warning_group_id=warning_group_id,
        priority=priority,
        worker_group=worker_group,
        tenant_code=tenant_code,
        environment_code=environment_code,
    )
    return run_with_service_runtime(
        env_file,
        _create_schedule_result,
        workflow=workflow,
        project=project,
        schedule_input=schedule_input,
        confirm_risk=confirm_risk,
    )


def update_schedule_result(
    schedule_id: int,
    *,
    cron: str | None = None,
    start: str | None = None,
    end: str | None = None,
    timezone: str | None = None,
    failure_strategy: FailureStrategyValue | None = None,
    warning_type: WarningTypeValue | None = None,
    warning_group_id: int | None = None,
    priority: PriorityValue | None = None,
    worker_group: str | None = None,
    environment_code: int | None = None,
    confirm_risk: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Update one schedule while preserving omitted fields."""
    require_positive_int(schedule_id, label="schedule id")
    if (
        cron is None
        and start is None
        and end is None
        and timezone is None
        and failure_strategy is None
        and warning_type is None
        and warning_group_id is None
        and priority is None
        and worker_group is None
        and environment_code is None
    ):
        message = "Schedule update requires at least one field change"
        raise UserInputError(
            message,
            suggestion=(
                "Pass at least one update flag such as --cron, --start, or --timezone."
            ),
        )

    return run_with_service_runtime(
        env_file,
        _update_schedule_result,
        schedule_id=schedule_id,
        cron=cron,
        start=start,
        end=end,
        timezone=timezone,
        failure_strategy=failure_strategy,
        warning_type=warning_type,
        warning_group_id=warning_group_id,
        priority=priority,
        worker_group=worker_group,
        environment_code=environment_code,
        confirm_risk=confirm_risk,
    )


def delete_schedule_result(
    schedule_id: int,
    *,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one schedule after explicit confirmation."""
    require_positive_int(schedule_id, label="schedule id")
    require_delete_force(force=force, resource_label="Schedule")
    return run_with_service_runtime(
        env_file,
        _delete_schedule_result,
        schedule_id=schedule_id,
    )


def online_schedule_result(
    schedule_id: int,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Bring one schedule online and return the refreshed payload."""
    require_positive_int(schedule_id, label="schedule id")
    return run_with_service_runtime(
        env_file,
        _online_schedule_result,
        schedule_id=schedule_id,
    )


def offline_schedule_result(
    schedule_id: int,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Bring one schedule offline and return the refreshed payload."""
    require_positive_int(schedule_id, label="schedule id")
    return run_with_service_runtime(
        env_file,
        _offline_schedule_result,
        schedule_id=schedule_id,
    )


def _list_schedules_result(
    runtime: ServiceRuntime,
    *,
    project: str | None,
    workflow: str | None,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    selected_workflow = (
        SelectedValue(value=workflow, source="flag") if workflow is not None else None
    )
    resolved_workflow = (
        resolve_workflow(
            selected_workflow.value,
            adapter=runtime.upstream.workflows,
            project_code=resolved_project.code,
        )
        if selected_workflow is not None
        else None
    )
    adapter = runtime.upstream.schedules
    data: SchedulePageData = requested_page_data(
        lambda current_page_no, current_page_size: adapter.list(
            project_code=resolved_project.code,
            workflow_code=(
                None if resolved_workflow is None else resolved_workflow.code
            ),
            search=search,
            page_no=current_page_no,
            page_size=current_page_size,
        ),
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=serialize_schedule,
        resource=SCHEDULE_RESOURCE,
        max_pages=MAX_AUTO_EXHAUST_PAGES,
        translate_error=lambda error: translate_schedule_api_error(
            error,
            operation="list",
            workflow_code=(
                None if resolved_workflow is None else resolved_workflow.code
            ),
            workflow_name=(
                None if resolved_workflow is None else resolved_workflow.name
            ),
        ),
    )

    resolved = {
        "project": _resolved_project_selection(
            resolved_project,
            selected_project,
        ),
        "search": search,
        "page_no": page_no,
        "page_size": page_size,
        "all": all_pages,
    }
    if selected_workflow is not None and resolved_workflow is not None:
        resolved["workflow"] = _resolved_workflow_selection(
            resolved_workflow,
            selected_workflow,
        )
    return CommandResult(
        data=require_json_object(data, label="schedule list data"),
        resolved=require_json_object(resolved, label="schedule list resolved"),
    )


def _get_schedule_result(
    runtime: ServiceRuntime,
    *,
    schedule_id: int,
) -> CommandResult:
    payload = _get_schedule(runtime, schedule_id=schedule_id)
    return CommandResult(
        data=require_json_object(serialize_schedule(payload), label="schedule data"),
        resolved={"schedule": {"id": schedule_id}},
    )


def _preview_schedule_result(
    runtime: ServiceRuntime,
    *,
    schedule_id: int,
) -> CommandResult:
    schedule = _get_schedule(runtime, schedule_id=schedule_id)
    project_code = _schedule_project_code(
        runtime,
        workflow_code=schedule.workflowDefinitionCode,
        operation="preview",
        schedule_id=schedule_id,
    )
    preview = preview_schedule(
        runtime,
        project_code=project_code,
        schedule_input=schedule_preview_input_from_payload(schedule),
    )
    return CommandResult(
        data=require_json_object(preview, label="schedule preview data"),
        resolved={"schedule": {"id": schedule_id}},
    )


def _preview_ad_hoc_schedule_result(
    runtime: ServiceRuntime,
    *,
    project: str | None,
    schedule_input: SchedulePreviewInput,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    preview = preview_schedule(
        runtime,
        project_code=resolved_project.code,
        schedule_input=schedule_input,
    )
    return CommandResult(
        data=require_json_object(preview, label="schedule preview data"),
        resolved={
            "project": _resolved_project_selection(
                resolved_project,
                selected_project,
            ),
        },
    )


def _explain_schedule_create_result(
    runtime: ServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
    schedule_input: ScheduleCreateDraft,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    effective_input = _schedule_input_with_runtime_defaults(
        schedule_input,
        runtime=runtime,
        project_code=resolved_project.code,
    )
    selected_workflow = require_workflow_selection(workflow, runtime=runtime)
    resolved_workflow = resolve_workflow(
        selected_workflow.value,
        adapter=runtime.upstream.workflows,
        project_code=resolved_project.code,
    )
    preview = preview_schedule(
        runtime,
        project_code=resolved_project.code,
        schedule_input=effective_input.schedule_input,
    )
    return CommandResult(
        data=require_json_object(
            _schedule_explain_data(
                action="schedule.create",
                preview=preview,
                proposed_schedule=_schedule_mutation_data_from_input(
                    effective_input.schedule_input
                ),
                schedule_payload=_schedule_create_confirmation_payload(
                    project_code=resolved_project.code,
                    workflow_code=resolved_workflow.code,
                    schedule_input=effective_input.schedule_input,
                ),
            ),
            label="schedule explain data",
        ),
        resolved={
            "project": _resolved_project_selection(
                resolved_project,
                selected_project,
            ),
            "workflow": _resolved_workflow_selection(
                resolved_workflow,
                selected_workflow,
            ),
            "tenant": selected_value_data(effective_input.tenant_selection),
            **_project_preference_resolution_data(effective_input),
        },
    )


def _explain_schedule_update_result(
    runtime: ServiceRuntime,
    *,
    schedule_id: int,
    cron: str | None,
    start: str | None,
    end: str | None,
    timezone: str | None,
    failure_strategy: FailureStrategyValue | None,
    warning_type: WarningTypeValue | None,
    warning_group_id: int | None,
    priority: PriorityValue | None,
    worker_group: str | None,
    environment_code: int | None,
) -> CommandResult:
    current = _get_schedule(runtime, schedule_id=schedule_id)
    requested_fields = _schedule_update_requested_fields(
        cron=cron,
        start=start,
        end=end,
        timezone=timezone,
        failure_strategy=failure_strategy,
        warning_type=warning_type,
        warning_group_id=warning_group_id,
        priority=priority,
        worker_group=worker_group,
        environment_code=environment_code,
    )
    schedule_input = _merged_schedule_update_input(
        current,
        cron=cron,
        start=start,
        end=end,
        timezone=timezone,
        failure_strategy=failure_strategy,
        warning_type=warning_type,
        warning_group_id=warning_group_id,
        priority=priority,
        worker_group=worker_group,
        environment_code=environment_code,
    )
    project_code = _schedule_project_code(
        runtime,
        workflow_code=current.workflowDefinitionCode,
        operation="explain",
        schedule_id=schedule_id,
    )
    preview = preview_schedule(
        runtime,
        project_code=project_code,
        schedule_input={
            "crontab": schedule_input["crontab"],
            "start_time": schedule_input["start_time"],
            "end_time": schedule_input["end_time"],
            "timezone_id": schedule_input["timezone_id"],
        },
    )
    current_schedule = _schedule_mutation_data_from_payload(current)
    proposed_schedule = _schedule_mutation_data_from_input(schedule_input)
    changed_fields, inherited_fields, unchanged_requested_fields = (
        _schedule_update_explain_field_groups(
            current_schedule=current_schedule,
            proposed_schedule=proposed_schedule,
            requested_fields=requested_fields,
        )
    )
    return CommandResult(
        data=require_json_object(
            _schedule_explain_data(
                action="schedule.update",
                preview=preview,
                proposed_schedule=proposed_schedule,
                schedule_payload=_schedule_update_confirmation_payload(
                    schedule_id=schedule_id,
                    schedule_input=schedule_input,
                ),
                current_schedule=current_schedule,
                requested_fields=requested_fields,
                changed_fields=changed_fields,
                inherited_fields=inherited_fields,
                unchanged_requested_fields=unchanged_requested_fields,
            ),
            label="schedule explain data",
        ),
        resolved={
            "schedule": {
                "id": schedule_id,
                "workflowDefinitionCode": current.workflowDefinitionCode,
                "workflowDefinitionName": current.workflowDefinitionName,
                "projectName": current.projectName,
                "projectCode": project_code,
            }
        },
    )


def _create_schedule_result(
    runtime: ServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
    schedule_input: ScheduleCreateDraft,
    confirm_risk: str | None,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    effective_input = _schedule_input_with_runtime_defaults(
        schedule_input,
        runtime=runtime,
        project_code=resolved_project.code,
    )
    selected_workflow = require_workflow_selection(workflow, runtime=runtime)
    resolved_workflow = resolve_workflow(
        selected_workflow.value,
        adapter=runtime.upstream.workflows,
        project_code=resolved_project.code,
    )
    confirmation_payload = _schedule_create_confirmation_payload(
        project_code=resolved_project.code,
        workflow_code=resolved_workflow.code,
        schedule_input=effective_input.schedule_input,
    )
    preview = preview_schedule(
        runtime,
        project_code=resolved_project.code,
        schedule_input=effective_input.schedule_input,
    )
    require_high_frequency_confirmation(
        action="schedule.create",
        confirmation=confirm_risk,
        preview=preview,
        schedule_payload=confirmation_payload,
    )
    try:
        payload = runtime.upstream.schedules.create(
            workflow_code=resolved_workflow.code,
            crontab=effective_input.schedule_input["crontab"],
            start_time=effective_input.schedule_input["start_time"],
            end_time=effective_input.schedule_input["end_time"],
            timezone_id=effective_input.schedule_input["timezone_id"],
            failure_strategy=effective_input.schedule_input["failure_strategy"],
            warning_type=effective_input.schedule_input["warning_type"],
            warning_group_id=effective_input.schedule_input["warning_group_id"],
            workflow_instance_priority=effective_input.schedule_input[
                "workflow_instance_priority"
            ],
            worker_group=effective_input.schedule_input["worker_group"],
            tenant_code=effective_input.schedule_input["tenant_code"],
            environment_code=effective_input.schedule_input["environment_code"],
        )
    except ApiResultError as error:
        raise translate_schedule_api_error(
            error,
            operation="create",
            workflow_code=resolved_workflow.code,
            workflow_name=resolved_workflow.name,
        ) from error
    warnings = confirmed_preview_warnings(preview)
    return CommandResult(
        data=require_json_object(serialize_schedule(payload), label="schedule data"),
        resolved={
            "project": _resolved_project_selection(
                resolved_project,
                selected_project,
            ),
            "workflow": _resolved_workflow_selection(
                resolved_workflow,
                selected_workflow,
            ),
            "tenant": selected_value_data(effective_input.tenant_selection),
            **_project_preference_resolution_data(effective_input),
        },
        warnings=warnings,
        warning_details=[
            require_json_object(detail, label="schedule warning detail")
            for detail in confirmed_preview_warning_details(preview)
        ],
    )


def _update_schedule_result(
    runtime: ServiceRuntime,
    *,
    schedule_id: int,
    cron: str | None,
    start: str | None,
    end: str | None,
    timezone: str | None,
    failure_strategy: FailureStrategyValue | None,
    warning_type: WarningTypeValue | None,
    warning_group_id: int | None,
    priority: PriorityValue | None,
    worker_group: str | None,
    environment_code: int | None,
    confirm_risk: str | None,
) -> CommandResult:
    adapter = runtime.upstream.schedules
    try:
        current = adapter.get(schedule_id=schedule_id)
        schedule_input = _merged_schedule_update_input(
            current,
            cron=cron,
            start=start,
            end=end,
            timezone=timezone,
            failure_strategy=failure_strategy,
            warning_type=warning_type,
            warning_group_id=warning_group_id,
            priority=priority,
            worker_group=worker_group,
            environment_code=environment_code,
        )
        preview = preview_schedule(
            runtime,
            project_code=_schedule_project_code(
                runtime,
                workflow_code=current.workflowDefinitionCode,
                operation="update",
                schedule_id=schedule_id,
            ),
            schedule_input={
                "crontab": schedule_input["crontab"],
                "start_time": schedule_input["start_time"],
                "end_time": schedule_input["end_time"],
                "timezone_id": schedule_input["timezone_id"],
            },
        )
        require_high_frequency_confirmation(
            action="schedule.update",
            confirmation=confirm_risk,
            preview=preview,
            schedule_payload=_schedule_update_confirmation_payload(
                schedule_id=schedule_id,
                schedule_input=schedule_input,
            ),
        )
        payload = adapter.update(
            schedule_id=schedule_id,
            crontab=schedule_input["crontab"],
            start_time=schedule_input["start_time"],
            end_time=schedule_input["end_time"],
            timezone_id=schedule_input["timezone_id"],
            failure_strategy=schedule_input["failure_strategy"],
            warning_type=schedule_input["warning_type"],
            warning_group_id=schedule_input["warning_group_id"],
            workflow_instance_priority=schedule_input["workflow_instance_priority"],
            worker_group=schedule_input["worker_group"],
            environment_code=schedule_input["environment_code"],
        )
    except ApiResultError as error:
        raise translate_schedule_api_error(
            error,
            operation="update",
            schedule_id=schedule_id,
        ) from error
    warnings = confirmed_preview_warnings(preview)
    return CommandResult(
        data=require_json_object(serialize_schedule(payload), label="schedule data"),
        resolved={"schedule": {"id": schedule_id}},
        warnings=warnings,
        warning_details=[
            require_json_object(detail, label="schedule warning detail")
            for detail in confirmed_preview_warning_details(preview)
        ],
    )


def _delete_schedule_result(
    runtime: ServiceRuntime,
    *,
    schedule_id: int,
) -> CommandResult:
    adapter = runtime.upstream.schedules
    try:
        current = adapter.get(schedule_id=schedule_id)
        deleted = adapter.delete(schedule_id=schedule_id)
    except ApiResultError as error:
        raise translate_schedule_api_error(
            error,
            operation="delete",
            schedule_id=schedule_id,
        ) from error
    return CommandResult(
        data=require_json_object(
            DeleteScheduleData(
                deleted=deleted,
                schedule=serialize_schedule(current),
            ),
            label="schedule delete data",
        ),
        resolved={"schedule": {"id": schedule_id}},
    )


def _online_schedule_result(
    runtime: ServiceRuntime,
    *,
    schedule_id: int,
) -> CommandResult:
    try:
        payload = runtime.upstream.schedules.online(schedule_id=schedule_id)
    except ApiResultError as error:
        raise translate_schedule_api_error(
            error,
            operation="online",
            schedule_id=schedule_id,
        ) from error
    return CommandResult(
        data=require_json_object(serialize_schedule(payload), label="schedule data"),
        resolved={"schedule": {"id": schedule_id}},
    )


def _offline_schedule_result(
    runtime: ServiceRuntime,
    *,
    schedule_id: int,
) -> CommandResult:
    try:
        payload = runtime.upstream.schedules.offline(schedule_id=schedule_id)
    except ApiResultError as error:
        raise translate_schedule_api_error(
            error,
            operation="offline",
            schedule_id=schedule_id,
        ) from error
    return CommandResult(
        data=require_json_object(serialize_schedule(payload), label="schedule data"),
        resolved={"schedule": {"id": schedule_id}},
    )


def _schedule_explain_data(
    *,
    action: str,
    preview: SchedulePreviewData,
    proposed_schedule: ScheduleMutationData,
    schedule_payload: dict[str, object],
    current_schedule: ScheduleMutationData | None = None,
    requested_fields: list[ScheduleExplainField] | None = None,
    changed_fields: list[ScheduleExplainField] | None = None,
    inherited_fields: list[ScheduleExplainField] | None = None,
    unchanged_requested_fields: list[ScheduleExplainField] | None = None,
) -> ExplainScheduleData:
    data = ExplainScheduleData(
        mutationAction=action,
        proposedSchedule=proposed_schedule,
        preview=preview,
        confirmation=schedule_confirmation_data(
            action=action,
            preview=preview,
            schedule_payload=schedule_payload,
        ),
    )
    if current_schedule is not None:
        data["currentSchedule"] = current_schedule
    if requested_fields is not None:
        data["requestedFields"] = list(requested_fields)
    if changed_fields is not None:
        data["changedFields"] = list(changed_fields)
    if inherited_fields is not None:
        data["inheritedFields"] = list(inherited_fields)
    if unchanged_requested_fields is not None:
        data["unchangedRequestedFields"] = list(unchanged_requested_fields)
    return data


def _schedule_mutation_data_from_input(
    schedule_input: ScheduleCreateInput,
) -> ScheduleMutationData:
    return schedule_mutation_data(
        crontab=schedule_input["crontab"],
        start_time=schedule_input["start_time"],
        end_time=schedule_input["end_time"],
        timezone_id=schedule_input["timezone_id"],
        failure_strategy=schedule_input["failure_strategy"],
        warning_type=schedule_input["warning_type"],
        warning_group_id=schedule_input["warning_group_id"],
        workflow_instance_priority=schedule_input["workflow_instance_priority"],
        worker_group=schedule_input["worker_group"],
        tenant_code=schedule_input["tenant_code"],
        environment_code=schedule_input["environment_code"],
    )


def _schedule_mutation_data_from_payload(
    schedule: SchedulePayloadRecord,
) -> ScheduleMutationData:
    return schedule_mutation_data(
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
        failure_strategy=enum_value(schedule.failureStrategy),
        warning_type=enum_value(schedule.warningType),
        warning_group_id=schedule.warningGroupId,
        workflow_instance_priority=enum_value(schedule.workflowInstancePriority),
        worker_group=schedule.workerGroup,
        tenant_code=schedule.tenantCode,
        environment_code=_current_environment_code(schedule),
    )


def _schedule_update_requested_fields(
    *,
    cron: str | None,
    start: str | None,
    end: str | None,
    timezone: str | None,
    failure_strategy: FailureStrategyValue | None,
    warning_type: WarningTypeValue | None,
    warning_group_id: int | None,
    priority: PriorityValue | None,
    worker_group: str | None,
    environment_code: int | None,
) -> list[ScheduleExplainField]:
    candidates: tuple[tuple[ScheduleExplainField, bool], ...] = (
        ("crontab", cron is not None),
        ("startTime", start is not None),
        ("endTime", end is not None),
        ("timezoneId", timezone is not None),
        ("failureStrategy", failure_strategy is not None),
        ("warningType", warning_type is not None),
        ("warningGroupId", warning_group_id is not None),
        ("workflowInstancePriority", priority is not None),
        ("workerGroup", worker_group is not None),
        ("environmentCode", environment_code is not None),
    )
    return [field for field, requested in candidates if requested]


def _schedule_update_explain_field_groups(
    *,
    current_schedule: ScheduleMutationData,
    proposed_schedule: ScheduleMutationData,
    requested_fields: list[ScheduleExplainField],
) -> ScheduleExplainFieldGroups:
    changed_fields: list[ScheduleExplainField] = []
    inherited_fields: list[ScheduleExplainField] = []
    unchanged_requested_fields: list[ScheduleExplainField] = []
    requested = set(requested_fields)
    for field in _SCHEDULE_EXPLAIN_FIELDS:
        if field not in requested:
            inherited_fields.append(field)
            continue
        if _schedule_explain_field_value(
            current_schedule,
            field,
        ) == _schedule_explain_field_value(proposed_schedule, field):
            unchanged_requested_fields.append(field)
            continue
        changed_fields.append(field)
    return changed_fields, inherited_fields, unchanged_requested_fields


def _schedule_explain_field_value(
    schedule: ScheduleMutationData,
    field: ScheduleExplainField,
) -> ScheduleExplainFieldValue:
    fields = cast("Mapping[str, ScheduleExplainFieldValue]", schedule)
    return fields[field]


def _schedule_input_with_runtime_defaults(
    schedule_input: ScheduleCreateDraft,
    *,
    runtime: ServiceRuntime,
    project_code: int,
) -> _EffectiveScheduleCreateInput:
    project_preference = load_project_preference_defaults(
        runtime,
        project_code=project_code,
    )
    project_preference_overrides = _project_preference_schedule_overrides(
        schedule_input,
        project_preference=project_preference,
    )
    tenant_selection = select_tenant_code(
        schedule_input["tenant_code"],
        runtime=runtime,
        project_preference=project_preference,
    )
    project_preference_used_fields = list(project_preference_overrides.used_fields)
    if tenant_selection.source == "project_preference":
        project_preference_used_fields.append("tenantCode")

    return _EffectiveScheduleCreateInput(
        schedule_input=validated_schedule_create_input(
            cron=schedule_input["crontab"],
            start=schedule_input["start_time"],
            end=schedule_input["end_time"],
            timezone=schedule_input["timezone_id"],
            failure_strategy=schedule_input["failure_strategy"],
            warning_type=project_preference_overrides.warning_type,
            warning_group_id=(
                0
                if project_preference_overrides.warning_group_id is None
                else project_preference_overrides.warning_group_id
            ),
            priority=project_preference_overrides.workflow_instance_priority,
            worker_group=project_preference_overrides.worker_group,
            tenant_code=tenant_selection.value,
            environment_code=(
                0
                if project_preference_overrides.environment_code is None
                else project_preference_overrides.environment_code
            ),
        ),
        tenant_selection=tenant_selection,
        project_preference_used_fields=tuple(project_preference_used_fields),
    )


def _project_preference_resolution_data(
    effective_input: _EffectiveScheduleCreateInput,
) -> JsonObject:
    if not effective_input.project_preference_used_fields:
        return {}
    return {
        "project_preference": {
            "used_fields": list(effective_input.project_preference_used_fields),
        }
    }


def _project_preference_schedule_overrides(
    schedule_input: ScheduleCreateDraft,
    *,
    project_preference: ProjectPreferenceDefaults | None,
) -> _ProjectPreferenceScheduleOverrides:
    if project_preference is None:
        return _ProjectPreferenceScheduleOverrides(
            warning_type=schedule_input["warning_type"],
            warning_group_id=schedule_input["warning_group_id"],
            workflow_instance_priority=schedule_input["workflow_instance_priority"],
            worker_group=schedule_input["worker_group"],
            environment_code=schedule_input["environment_code"],
            used_fields=(),
        )

    used_fields: list[str] = []
    warning_type = _schedule_override_text(
        current_value=schedule_input["warning_type"],
        project_preference_value=project_preference.warning_type,
        output_field="warningType",
        used_fields=used_fields,
    )
    warning_group_id = _schedule_override_int(
        current_value=schedule_input["warning_group_id"],
        project_preference_value=project_preference.warning_group_id,
        output_field="warningGroupId",
        used_fields=used_fields,
    )
    workflow_instance_priority = _schedule_override_text(
        current_value=schedule_input["workflow_instance_priority"],
        project_preference_value=project_preference.task_priority,
        output_field="workflowInstancePriority",
        used_fields=used_fields,
    )
    worker_group = _schedule_override_text(
        current_value=schedule_input["worker_group"],
        project_preference_value=project_preference.worker_group,
        output_field="workerGroup",
        used_fields=used_fields,
    )
    environment_code = _schedule_override_int(
        current_value=schedule_input["environment_code"],
        project_preference_value=project_preference.environment_code,
        output_field="environmentCode",
        used_fields=used_fields,
    )
    return _ProjectPreferenceScheduleOverrides(
        warning_type=warning_type,
        warning_group_id=warning_group_id,
        workflow_instance_priority=workflow_instance_priority,
        worker_group=worker_group,
        environment_code=environment_code,
        used_fields=tuple(used_fields),
    )


def _schedule_override_text(
    *,
    current_value: str | None,
    project_preference_value: str | None,
    output_field: str,
    used_fields: list[str],
) -> str | None:
    if current_value is not None or project_preference_value is None:
        return current_value
    used_fields.append(output_field)
    return project_preference_value


def _schedule_override_int(
    *,
    current_value: int | None,
    project_preference_value: int | None,
    output_field: str,
    used_fields: list[str],
) -> int | None:
    if current_value is not None or project_preference_value is None:
        return current_value
    used_fields.append(output_field)
    return project_preference_value


def _schedule_create_confirmation_payload(
    *,
    project_code: int,
    workflow_code: int,
    schedule_input: ScheduleCreateInput,
) -> dict[str, object]:
    return {
        "environment_code": schedule_input["environment_code"],
        "project_code": project_code,
        "schedule": schedule_input,
        "workflow_code": workflow_code,
    }


def _schedule_update_confirmation_payload(
    *,
    schedule_id: int,
    schedule_input: ScheduleCreateInput,
) -> dict[str, object]:
    return {
        "schedule_id": schedule_id,
        "schedule": {
            "crontab": schedule_input["crontab"],
            "start_time": schedule_input["start_time"],
            "end_time": schedule_input["end_time"],
            "timezone_id": schedule_input["timezone_id"],
        },
    }


def _merged_schedule_update_input(
    current: SchedulePayloadRecord,
    *,
    cron: str | None,
    start: str | None,
    end: str | None,
    timezone: str | None,
    failure_strategy: FailureStrategyValue | None,
    warning_type: WarningTypeValue | None,
    warning_group_id: int | None,
    priority: PriorityValue | None,
    worker_group: str | None,
    environment_code: int | None,
) -> ScheduleCreateInput:
    return {
        "crontab": (
            require_quartz_cron_text(cron, label="cron")
            if cron is not None
            else required_update_text(
                None,
                fallback=current.crontab,
                label="cron",
            )
        ),
        "start_time": required_update_text(
            start,
            fallback=current.startTime,
            label="start",
        ),
        "end_time": required_update_text(
            end,
            fallback=current.endTime,
            label="end",
        ),
        "timezone_id": required_update_text(
            timezone,
            fallback=current.timezoneId,
            label="timezone",
        ),
        "failure_strategy": updated_optional_enum(
            failure_strategy,
            current=enum_value(current.failureStrategy),
            allowed=frozenset({"CONTINUE", "END"}),
            label="failure_strategy",
        ),
        "warning_type": updated_optional_enum(
            warning_type,
            current=enum_value(current.warningType),
            allowed=frozenset({"NONE", "SUCCESS", "FAILURE", "ALL"}),
            label="warning_type",
        ),
        "warning_group_id": (
            current.warningGroupId
            if warning_group_id is None
            else require_non_negative_int(
                warning_group_id,
                label="warning_group_id",
            )
        ),
        "workflow_instance_priority": updated_optional_enum(
            priority,
            current=enum_value(current.workflowInstancePriority),
            allowed=frozenset({"HIGHEST", "HIGH", "MEDIUM", "LOW", "LOWEST"}),
            label="priority",
        ),
        "worker_group": (
            current.workerGroup if worker_group is None else optional_text(worker_group)
        ),
        "tenant_code": current.tenantCode,
        "environment_code": (
            _current_environment_code(current)
            if environment_code is None
            else require_non_negative_int(
                environment_code,
                label="environment_code",
            )
        ),
    }


def _required_text_option(value: str | None, *, label: str) -> str:
    if value is None:
        message = f"schedule explain requires --{label}"
        raise UserInputError(
            message,
            suggestion=(
                f"Pass `--{label}` together with the other schedule fields, or "
                "provide a schedule id to explain an existing schedule."
            ),
        )
    return value


def _resolved_project_selection(
    project: ResolvedProject,
    selection: SelectedValue,
) -> dict[str, int | str | None]:
    return with_selection_source(cast("SelectionData", project.to_data()), selection)


def _resolved_workflow_selection(
    workflow: ResolvedWorkflow,
    selection: SelectedValue,
) -> dict[str, int | str | None]:
    return with_selection_source(cast("SelectionData", workflow.to_data()), selection)


def _get_schedule(
    runtime: ServiceRuntime,
    *,
    schedule_id: int,
) -> SchedulePayloadRecord:
    try:
        return runtime.upstream.schedules.get(schedule_id=schedule_id)
    except ApiResultError as error:
        raise translate_schedule_api_error(
            error,
            operation="get",
            schedule_id=schedule_id,
        ) from error


def _schedule_project_code(
    runtime: ServiceRuntime,
    *,
    workflow_code: int,
    operation: str,
    schedule_id: int | None = None,
) -> int:
    try:
        return runtime.upstream.workflows.get(code=workflow_code).projectCode
    except ApiResultError as error:
        raise translate_schedule_api_error(
            error,
            operation=operation,
            schedule_id=schedule_id,
            workflow_code=workflow_code,
        ) from error


def _current_environment_code(schedule: SchedulePayloadRecord) -> int:
    current = schedule.environmentCode
    if current is None:
        return 0
    return current
