from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Literal, TypeAlias, TypeVar, cast

from dsctl.cli_surface import SCHEDULE_RESOURCE
from dsctl.errors import ApiResultError, UserInputError
from dsctl.output import CommandResult, require_json_object
from dsctl.services._schedule_environment_guard import (
    require_schedule_environment_inheritance,
    schedule_environment_warning,
)
from dsctl.services._schedule_support import (
    FailureStrategyValue,
    PriorityValue,
    ScheduleCreateDraft,
    SchedulePreviewInput,
    WarningTypeValue,
    confirmed_preview_warning_details,
    confirmed_preview_warnings,
    normalize_environment_code,
    require_high_frequency_confirmation,
    schedule_confirmation_data,
    translate_schedule_api_error,
    validated_optional_enum,
    validated_schedule_create_draft,
    validated_schedule_preview_input,
)
from dsctl.services._validation import (
    require_delete_force,
    require_non_empty_text,
    require_non_negative_int,
    require_positive_int,
    require_quartz_cron_text,
)
from dsctl.services.runtime import (
    BoundDomainServiceRuntime,
    run_with_bound_domain_service_runtime,
)
from dsctl.services.schedule_analysis import build_schedule_preview_data
from dsctl.services.selection import (
    SelectedValue,
    require_project_selection,
    require_workflow_selection,
    selected_value_data,
)
from dsctl.upstream.pagination import DEFAULT_PAGE_SIZE, render_page_data
from dsctl.upstream.schedules import (
    SCHEDULE_DOMAIN,
    PreparedScheduleCreate,
    PreparedScheduleUpdate,
    ScheduleDomain,
    ScheduleOperations,
    ScheduleSnapshot,
    ScheduleState,
    ScheduleUpdatePatch,
)
from dsctl.upstream.serialization import optional_text

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from dsctl.output import JsonObject, JsonValue
    from dsctl.services.schedule_analysis import SchedulePreviewData


ScheduleServiceRuntime: TypeAlias = BoundDomainServiceRuntime[ScheduleDomain]
ScheduleDraftValue: TypeAlias = str | int | None
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

_EXPLAIN_FIELDS: tuple[ScheduleExplainField, ...] = (
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
    return run_with_bound_domain_service_runtime(
        env_file,
        SCHEDULE_DOMAIN,
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
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Get one schedule by id."""
    require_positive_int(schedule_id, label="schedule id")
    return run_with_bound_domain_service_runtime(
        env_file,
        SCHEDULE_DOMAIN,
        _get_schedule_result,
        schedule_id=schedule_id,
        project=project,
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
        if any(value is not None for value in (cron, start, end, timezone)):
            message = "schedule id preview does not accept schedule fields"
            raise UserInputError(
                message,
                suggestion=(
                    "Pass only the schedule id and optional `--project` to preview "
                    "an existing schedule, or omit the id and pass `--project`, "
                    "`--cron`, `--start`, and `--end` (plus `--timezone` where "
                    "supported)."
                ),
            )
        return run_with_bound_domain_service_runtime(
            env_file,
            SCHEDULE_DOMAIN,
            _preview_existing_schedule_result,
            schedule_id=schedule_id,
            project=project,
        )

    schedule_input = validated_schedule_preview_input(
        cron=cron,
        start=start,
        end=end,
        timezone=timezone,
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        SCHEDULE_DOMAIN,
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
    missed_fire_policy: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Explain one schedule mutation without changing remote state."""
    if schedule_id is None:
        schedule_input = validated_schedule_create_draft(
            cron=_required_option(cron, label="cron"),
            start=_required_option(start, label="start"),
            end=_required_option(end, label="end"),
            timezone=timezone,
            failure_strategy=failure_strategy,
            warning_type=warning_type,
            warning_group_id=warning_group_id,
            priority=priority,
            worker_group=worker_group,
            tenant_code=tenant_code,
            environment_code=environment_code,
            missed_fire_policy=missed_fire_policy,
        )
        return run_with_bound_domain_service_runtime(
            env_file,
            SCHEDULE_DOMAIN,
            _explain_schedule_create_result,
            workflow=workflow,
            project=project,
            schedule_input=schedule_input,
        )

    require_positive_int(schedule_id, label="schedule id")
    if workflow is not None or tenant_code is not None:
        message = "schedule explain by id does not accept --workflow or --tenant-code"
        raise UserInputError(
            message,
            suggestion=(
                "Pass optional `--project` and update flags with SCHEDULE_ID; "
                "omit the id for create-style explain."
            ),
        )
    patch = _validated_update_patch(
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
        missed_fire_policy=missed_fire_policy,
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        SCHEDULE_DOMAIN,
        _explain_schedule_update_result,
        schedule_id=schedule_id,
        project=project,
        patch=patch,
    )


def create_schedule_result(
    *,
    workflow: str | None,
    project: str | None = None,
    cron: str,
    start: str,
    end: str,
    timezone: str | None = None,
    failure_strategy: FailureStrategyValue | None = None,
    warning_type: WarningTypeValue | None = None,
    warning_group_id: int | None = None,
    priority: PriorityValue | None = None,
    worker_group: str | None = None,
    tenant_code: str | None = None,
    environment_code: int | None = None,
    missed_fire_policy: str | None = None,
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
        missed_fire_policy=missed_fire_policy,
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        SCHEDULE_DOMAIN,
        _create_schedule_result,
        workflow=workflow,
        project=project,
        schedule_input=schedule_input,
        confirm_risk=confirm_risk,
    )


def update_schedule_result(
    schedule_id: int,
    *,
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
    environment_code: int | None = None,
    missed_fire_policy: str | None = None,
    confirm_risk: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Update one schedule while preserving omitted fields."""
    require_positive_int(schedule_id, label="schedule id")
    patch = _validated_update_patch(
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
        missed_fire_policy=missed_fire_policy,
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        SCHEDULE_DOMAIN,
        _update_schedule_result,
        schedule_id=schedule_id,
        project=project,
        patch=patch,
        confirm_risk=confirm_risk,
    )


def delete_schedule_result(
    schedule_id: int,
    *,
    force: bool,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one schedule after explicit confirmation."""
    require_positive_int(schedule_id, label="schedule id")
    require_delete_force(force=force, resource_label="Schedule")
    return run_with_bound_domain_service_runtime(
        env_file,
        SCHEDULE_DOMAIN,
        _delete_schedule_result,
        schedule_id=schedule_id,
        project=project,
    )


def online_schedule_result(
    schedule_id: int,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Bring one schedule online and return the refreshed payload."""
    return _run_lifecycle(
        schedule_id,
        operation="online",
        project=project,
        env_file=env_file,
    )


def offline_schedule_result(
    schedule_id: int,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Bring one schedule offline and return the refreshed payload."""
    return _run_lifecycle(
        schedule_id,
        operation="offline",
        project=project,
        env_file=env_file,
    )


def _list_schedules_result(
    runtime: ScheduleServiceRuntime,
    *,
    project: str | None,
    workflow: str | None,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    selected_workflow = (
        None if workflow is None else SelectedValue(workflow, source="flag")
    )
    try:
        listing = runtime.domain.schedules.list(
            selected_project.value,
            workflow_selector=workflow,
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
        )
    except ApiResultError as error:
        raise translate_schedule_api_error(error, operation="list") from error
    data = render_page_data(
        listing.page,
        serialize_item=lambda item: item.to_data(),
        resource=SCHEDULE_RESOURCE,
    )
    resolved: JsonObject = {
        "project": _selected_ref(
            listing.project.to_data(),
            selected_project,
        ),
        "search": search,
        "page_no": page_no,
        "page_size": page_size,
        "all": all_pages,
    }
    if selected_workflow is not None and listing.workflow is not None:
        resolved["workflow"] = _selected_ref(
            listing.workflow.to_data(),
            selected_workflow,
        )
    return _with_environment_warnings(
        CommandResult(
            data=require_json_object(data, label="schedule list data"),
            resolved=resolved,
        ),
        listing.page.totalList or (),
    )


def _get_schedule_result(
    runtime: ScheduleServiceRuntime,
    *,
    schedule_id: int,
    project: str | None,
) -> CommandResult:
    selected_project = _optional_project_selection(project, runtime=runtime)
    located = _schedule_call(
        "get",
        schedule_id,
        lambda: runtime.domain.schedules.get(
            schedule_id,
            project_selector=_project_selector(selected_project),
        ),
    )
    return _with_environment_warning(
        CommandResult(
            data=located.schedule.to_data(),
            resolved=_id_scope_resolved(schedule_id, selected_project),
        ),
        located.schedule,
    )


def _preview_existing_schedule_result(
    runtime: ScheduleServiceRuntime,
    *,
    schedule_id: int,
    project: str | None,
) -> CommandResult:
    selected_project = _optional_project_selection(project, runtime=runtime)
    located, times = _schedule_call(
        "preview",
        schedule_id,
        lambda: runtime.domain.schedules.preview_existing(
            schedule_id,
            project_selector=_project_selector(selected_project),
        ),
    )
    return _with_environment_warning(
        CommandResult(
            data=require_json_object(
                build_schedule_preview_data(list(times)),
                label="schedule preview data",
            ),
            resolved=_with_project_selection(
                {
                    "schedule": {
                        "id": schedule_id,
                        "project": located.project.to_data(),
                    }
                },
                selected_project,
            ),
        ),
        located.schedule,
    )


def _preview_ad_hoc_schedule_result(
    runtime: ScheduleServiceRuntime,
    *,
    project: str | None,
    schedule_input: SchedulePreviewInput,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    state = _preview_state(schedule_input)
    try:
        preview_result = runtime.domain.schedules.preview(
            selected_project.value,
            state,
        )
    except ApiResultError as error:
        raise translate_schedule_api_error(error, operation="preview") from error
    return CommandResult(
        data=require_json_object(
            build_schedule_preview_data(list(preview_result.times)),
            label="schedule preview data",
        ),
        resolved={
            "project": _selected_ref(
                preview_result.project.to_data(),
                selected_project,
            )
        },
    )


def _explain_schedule_create_result(
    runtime: ScheduleServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
    schedule_input: ScheduleCreateDraft,
) -> CommandResult:
    selected_project, selected_workflow = _create_selections(
        runtime,
        project=project,
        workflow=workflow,
    )
    prepared = _prepare_create(
        runtime.domain.schedules,
        selected_project,
        selected_workflow,
        schedule_input,
    )
    require_schedule_environment_inheritance(
        runtime.profile.ds_version, prepared.state.environment_code
    )
    preview = build_schedule_preview_data(list(prepared.preview_times))
    proposed = prepared.state.to_data()
    data = _explain_data(
        action="schedule.create",
        preview=preview,
        proposed=proposed,
        confirmation_payload=_create_confirmation_payload(prepared),
    )
    return CommandResult(
        data=data,
        resolved=_create_resolved(
            prepared,
            selected_project=selected_project,
            selected_workflow=selected_workflow,
        ),
    )


def _explain_schedule_update_result(
    runtime: ScheduleServiceRuntime,
    *,
    schedule_id: int,
    project: str | None,
    patch: ScheduleUpdatePatch,
) -> CommandResult:
    selected_project = _optional_project_selection(project, runtime=runtime)
    prepared = _prepare_update(
        runtime.domain.schedules,
        schedule_id,
        patch,
        operation="explain",
        project_selector=_project_selector(selected_project),
    )
    if "environmentCode" in prepared.requested_fields:
        require_schedule_environment_inheritance(
            runtime.profile.ds_version, prepared.state.environment_code
        )
    preview = build_schedule_preview_data(list(prepared.preview_times))
    current = prepared.current.schedule.state().to_data()
    proposed = prepared.state.to_data()
    requested, changed, inherited, unchanged = _update_field_groups(
        current=current,
        proposed=proposed,
        requested=prepared.requested_fields,
    )
    data = _explain_data(
        action="schedule.update",
        preview=preview,
        proposed=proposed,
        confirmation_payload=_update_confirmation_payload(prepared),
        current=current,
        requested=requested,
        changed=changed,
        inherited=inherited,
        unchanged=unchanged,
    )
    return _with_environment_warning(
        CommandResult(
            data=data,
            resolved=_with_project_selection(
                {"schedule": _resolved_schedule(prepared)},
                selected_project,
            ),
        ),
        prepared.current.schedule,
    )


def _create_schedule_result(
    runtime: ScheduleServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
    schedule_input: ScheduleCreateDraft,
    confirm_risk: str | None,
) -> CommandResult:
    selected_project, selected_workflow = _create_selections(
        runtime,
        project=project,
        workflow=workflow,
    )
    operations = runtime.domain.schedules
    prepared = _prepare_create(
        operations,
        selected_project,
        selected_workflow,
        schedule_input,
    )
    require_schedule_environment_inheritance(
        runtime.profile.ds_version, prepared.state.environment_code
    )
    preview = build_schedule_preview_data(list(prepared.preview_times))
    require_high_frequency_confirmation(
        action="schedule.create",
        confirmation=confirm_risk,
        preview=preview,
        schedule_payload=_create_confirmation_payload(prepared),
    )
    try:
        created = operations.create(prepared)
    except ApiResultError as error:
        raise translate_schedule_api_error(
            error,
            operation="create",
            workflow_code=prepared.scope.workflow.native.value,
            workflow_name=prepared.scope.workflow.name,
            environment_code=prepared.state.environment_code,
        ) from error
    return _mutation_result(
        created,
        preview=preview,
        resolved=_create_resolved(
            prepared,
            selected_project=selected_project,
            selected_workflow=selected_workflow,
        ),
    )


def _update_schedule_result(
    runtime: ScheduleServiceRuntime,
    *,
    schedule_id: int,
    project: str | None,
    patch: ScheduleUpdatePatch,
    confirm_risk: str | None,
) -> CommandResult:
    operations = runtime.domain.schedules
    selected_project = _optional_project_selection(project, runtime=runtime)
    prepared = _prepare_update(
        operations,
        schedule_id,
        patch,
        operation="update",
        project_selector=_project_selector(selected_project),
    )
    if "environmentCode" in prepared.requested_fields:
        require_schedule_environment_inheritance(
            runtime.profile.ds_version, prepared.state.environment_code
        )
    preview = build_schedule_preview_data(list(prepared.preview_times))
    require_high_frequency_confirmation(
        action="schedule.update",
        confirmation=confirm_risk,
        preview=preview,
        schedule_payload=_update_confirmation_payload(prepared),
    )
    try:
        updated = operations.update(prepared)
    except ApiResultError as error:
        raise translate_schedule_api_error(
            error,
            operation="update",
            schedule_id=schedule_id,
            environment_code=prepared.state.environment_code,
        ) from error
    return _mutation_result(
        updated,
        preview=preview,
        resolved=_id_scope_resolved(schedule_id, selected_project),
    )


def _delete_schedule_result(
    runtime: ScheduleServiceRuntime,
    *,
    schedule_id: int,
    project: str | None,
) -> CommandResult:
    selected_project = _optional_project_selection(project, runtime=runtime)
    try:
        current, deleted = runtime.domain.schedules.delete(
            schedule_id,
            project_selector=_project_selector(selected_project),
        )
    except ApiResultError as error:
        raise translate_schedule_api_error(
            error,
            operation="delete",
            schedule_id=schedule_id,
        ) from error
    return CommandResult(
        data={"deleted": deleted, "schedule": current.to_data()},
        resolved=_id_scope_resolved(schedule_id, selected_project),
    )


def _lifecycle_schedule_result(
    runtime: ScheduleServiceRuntime,
    *,
    schedule_id: int,
    operation: Literal["online", "offline"],
    project: str | None,
) -> CommandResult:
    selected_project = _optional_project_selection(project, runtime=runtime)
    try:
        method = (
            runtime.domain.schedules.online
            if operation == "online"
            else runtime.domain.schedules.offline
        )
        schedule = method(
            schedule_id,
            project_selector=_project_selector(selected_project),
        )
    except ApiResultError as error:
        raise translate_schedule_api_error(
            error,
            operation=operation,
            schedule_id=schedule_id,
        ) from error
    return _with_environment_warning(
        CommandResult(
            data=schedule.to_data(),
            resolved=_id_scope_resolved(schedule_id, selected_project),
        ),
        schedule,
    )


def _run_lifecycle(
    schedule_id: int,
    *,
    operation: Literal["online", "offline"],
    project: str | None,
    env_file: str | None,
) -> CommandResult:
    require_positive_int(schedule_id, label="schedule id")
    return run_with_bound_domain_service_runtime(
        env_file,
        SCHEDULE_DOMAIN,
        _lifecycle_schedule_result,
        schedule_id=schedule_id,
        operation=operation,
        project=project,
    )


def _prepare_create(
    operations: ScheduleOperations,
    selected_project: SelectedValue,
    selected_workflow: SelectedValue,
    schedule_input: ScheduleCreateDraft,
) -> PreparedScheduleCreate:
    state = _create_state(schedule_input)
    try:
        return operations.prepare_create(
            selected_project.value,
            selected_workflow.value,
            state,
        )
    except ApiResultError as error:
        raise translate_schedule_api_error(
            error,
            operation="create",
            environment_code=state.environment_code,
        ) from error


def _prepare_update(
    operations: ScheduleOperations,
    schedule_id: int,
    patch: ScheduleUpdatePatch,
    *,
    operation: Literal["explain", "update"],
    project_selector: str | None,
) -> PreparedScheduleUpdate:
    try:
        return operations.prepare_update(
            schedule_id,
            patch,
            project_selector=project_selector,
        )
    except ApiResultError as error:
        requested_environment = patch.environment_code
        environment_code = (
            requested_environment
            if isinstance(requested_environment, int)
            and not isinstance(requested_environment, bool)
            and requested_environment > 0
            else None
        )
        raise translate_schedule_api_error(
            error,
            operation=operation,
            schedule_id=schedule_id,
            environment_code=environment_code,
        ) from error


def _create_selections(
    runtime: ScheduleServiceRuntime,
    *,
    project: str | None,
    workflow: str | None,
) -> tuple[SelectedValue, SelectedValue]:
    selected_project = require_project_selection(project, runtime=runtime)
    selected_workflow = require_workflow_selection(
        workflow,
    )
    return selected_project, selected_workflow


def _optional_project_selection(
    project: str | None,
    *,
    runtime: ScheduleServiceRuntime,
) -> SelectedValue | None:
    """Use an explicit/context project when present without requiring one."""
    if project is None and runtime.context.project is None:
        return None
    return require_project_selection(project, runtime=runtime)


def _project_selector(selected: SelectedValue | None) -> str | None:
    return None if selected is None else selected.value


def _id_scope_resolved(
    schedule_id: int,
    selected_project: SelectedValue | None,
) -> JsonObject:
    return _with_project_selection(
        {"schedule": {"id": schedule_id}},
        selected_project,
    )


def _with_project_selection(
    resolved: JsonObject,
    selected_project: SelectedValue | None,
) -> JsonObject:
    if selected_project is not None:
        resolved["project"] = selected_value_data(selected_project)
    return resolved


def _create_state(schedule_input: ScheduleCreateDraft) -> ScheduleState:
    provided = {"crontab", "startTime", "endTime"}

    def supplied(value: ScheduleDraftValue, field: str) -> None:
        if value is not None:
            provided.add(field)

    supplied(schedule_input["timezone_id"], "timezoneId")
    supplied(schedule_input["failure_strategy"], "failureStrategy")
    supplied(schedule_input["warning_type"], "warningType")
    supplied(schedule_input["warning_group_id"], "warningGroupId")
    supplied(schedule_input["workflow_instance_priority"], "workflowInstancePriority")
    supplied(schedule_input["worker_group"], "workerGroup")
    supplied(schedule_input["tenant_code"], "tenantCode")
    supplied(schedule_input["environment_code"], "environmentCode")
    supplied(schedule_input.get("missed_fire_policy"), "missedFirePolicy")
    return ScheduleState(
        crontab=schedule_input["crontab"],
        start_time=schedule_input["start_time"],
        end_time=schedule_input["end_time"],
        timezone_id=schedule_input["timezone_id"],
        failure_strategy=schedule_input["failure_strategy"] or "CONTINUE",
        warning_type=schedule_input["warning_type"] or "NONE",
        warning_group_id=schedule_input["warning_group_id"] or 0,
        workflow_instance_priority=(
            schedule_input["workflow_instance_priority"] or "MEDIUM"
        ),
        worker_group=schedule_input["worker_group"] or "default",
        tenant_code=schedule_input["tenant_code"],
        environment_code=normalize_environment_code(schedule_input["environment_code"]),
        missed_fire_policy=schedule_input.get("missed_fire_policy"),
        provided_fields=frozenset(provided),
    )


def _preview_state(schedule_input: SchedulePreviewInput) -> ScheduleState:
    provided = {"crontab", "startTime", "endTime"}
    if schedule_input["timezone_id"] is not None:
        provided.add("timezoneId")
    return ScheduleState(
        crontab=schedule_input["crontab"],
        start_time=schedule_input["start_time"],
        end_time=schedule_input["end_time"],
        timezone_id=schedule_input["timezone_id"],
        provided_fields=frozenset(provided),
    )


def _validated_update_patch(
    *,
    cron: str | None,
    start: str | None,
    end: str | None,
    timezone: str | None,
    failure_strategy: str | None,
    warning_type: str | None,
    warning_group_id: int | None,
    priority: str | None,
    worker_group: str | None,
    environment_code: int | None,
    missed_fire_policy: str | None = None,
) -> ScheduleUpdatePatch:
    patch = _validated_timing_patch(
        cron=cron,
        start=start,
        end=end,
        timezone=timezone,
    )
    patch = _validated_policy_patch(
        patch,
        failure_strategy=failure_strategy,
        warning_type=warning_type,
        warning_group_id=warning_group_id,
        priority=priority,
        worker_group=worker_group,
        environment_code=environment_code,
        missed_fire_policy=missed_fire_policy,
    )
    if not patch.requested_fields():
        message = "Schedule update requires at least one field change"
        raise UserInputError(
            message,
            suggestion=(
                "Pass at least one update flag such as --cron, --start, or --timezone."
            ),
        )
    return patch


def _validated_timing_patch(
    *,
    cron: str | None,
    start: str | None,
    end: str | None,
    timezone: str | None,
) -> ScheduleUpdatePatch:
    patch = ScheduleUpdatePatch()
    if cron is not None:
        patch = replace(
            patch,
            crontab=require_quartz_cron_text(cron, label="cron"),
        )
    if start is not None:
        patch = replace(
            patch,
            start_time=require_non_empty_text(start, label="start"),
        )
    if end is not None:
        patch = replace(patch, end_time=require_non_empty_text(end, label="end"))
    if timezone is not None:
        patch = replace(
            patch,
            timezone_id=require_non_empty_text(timezone, label="timezone"),
        )
    return patch


def _validated_policy_patch(
    patch: ScheduleUpdatePatch,
    *,
    failure_strategy: str | None,
    warning_type: str | None,
    warning_group_id: int | None,
    priority: str | None,
    worker_group: str | None,
    environment_code: int | None,
    missed_fire_policy: str | None = None,
) -> ScheduleUpdatePatch:
    if missed_fire_policy is not None:
        patch = replace(
            patch,
            missed_fire_policy=require_non_empty_text(
                missed_fire_policy, label="missed_fire_policy"
            ).upper(),
        )
    if failure_strategy is not None:
        patch = replace(
            patch,
            failure_strategy=_required_enum(
                failure_strategy,
                allowed=frozenset({"CONTINUE", "END"}),
                label="failure_strategy",
            ),
        )
    if warning_type is not None:
        patch = replace(
            patch,
            warning_type=_required_enum(
                warning_type,
                allowed=frozenset({"NONE", "SUCCESS", "FAILURE", "ALL"}),
                label="warning_type",
            ),
        )
    if warning_group_id is not None:
        patch = replace(
            patch,
            warning_group_id=require_non_negative_int(
                warning_group_id,
                label="warning_group_id",
            ),
        )
    if priority is not None:
        patch = replace(
            patch,
            workflow_instance_priority=_required_enum(
                priority,
                allowed=frozenset({"HIGHEST", "HIGH", "MEDIUM", "LOW", "LOWEST"}),
                label="priority",
            ),
        )
    if worker_group is not None:
        patch = replace(
            patch,
            worker_group=require_non_empty_text(worker_group, label="worker_group"),
        )
    if environment_code is not None:
        patch = replace(
            patch,
            environment_code=normalize_environment_code(environment_code),
        )
    return patch


def _required_enum(
    value: str,
    *,
    allowed: frozenset[str],
    label: str,
) -> str:
    normalized = validated_optional_enum(value, allowed=allowed, label=label)
    if normalized is not None:
        return normalized
    message = f"{label} must not be empty"
    option = label.replace("_", "-")
    choices = ", ".join(sorted(allowed))
    raise UserInputError(
        message,
        suggestion=f"Pass `--{option}` as one of: {choices}.",
    )


def _required_option(value: str | None, *, label: str) -> str:
    if value is not None:
        return value
    message = f"schedule explain requires --{label}"
    raise UserInputError(
        message,
        suggestion=(
            f"Pass `--{label}` with the other create fields, or provide a "
            "schedule id to explain an update."
        ),
    )


def _create_confirmation_payload(
    prepared: PreparedScheduleCreate,
) -> JsonObject:
    return {
        "project": prepared.scope.project.to_data(),
        "workflow": prepared.scope.workflow.to_data(),
        "schedule": prepared.state.to_data(),
    }


def _update_confirmation_payload(
    prepared: PreparedScheduleUpdate,
) -> JsonObject:
    return {
        "schedule_id": prepared.current.schedule.id,
        "schedule": prepared.state.to_data(),
    }


def _explain_data(
    *,
    action: str,
    preview: SchedulePreviewData,
    proposed: JsonObject,
    confirmation_payload: JsonObject,
    current: JsonObject | None = None,
    requested: list[ScheduleExplainField] | None = None,
    changed: list[ScheduleExplainField] | None = None,
    inherited: list[ScheduleExplainField] | None = None,
    unchanged: list[ScheduleExplainField] | None = None,
) -> JsonObject:
    data: dict[str, JsonValue] = {
        "mutationAction": action,
        "proposedSchedule": proposed,
        "preview": cast("JsonValue", preview),
        "confirmation": cast(
            "JsonValue",
            schedule_confirmation_data(
                action=action,
                preview=preview,
                schedule_payload=confirmation_payload,
            ),
        ),
    }
    if current is not None:
        data["currentSchedule"] = current
        data["requestedFields"] = requested or []
        data["changedFields"] = changed or []
        data["inheritedFields"] = inherited or []
        data["unchangedRequestedFields"] = unchanged or []
    return require_json_object(data, label="schedule explain data")


def _update_field_groups(
    *,
    current: JsonObject,
    proposed: JsonObject,
    requested: frozenset[str],
) -> tuple[
    list[ScheduleExplainField],
    list[ScheduleExplainField],
    list[ScheduleExplainField],
    list[ScheduleExplainField],
]:
    ordered_requested = [field for field in _EXPLAIN_FIELDS if field in requested]
    changed = [
        field
        for field in ordered_requested
        if current.get(field) != proposed.get(field)
    ]
    unchanged = [field for field in ordered_requested if field not in changed]
    inherited = [field for field in _EXPLAIN_FIELDS if field not in requested]
    return ordered_requested, changed, inherited, unchanged


def _create_resolved(
    prepared: PreparedScheduleCreate,
    *,
    selected_project: SelectedValue,
    selected_workflow: SelectedValue,
) -> JsonObject:
    resolved: JsonObject = {
        "project": _selected_ref(
            prepared.scope.project.to_data(),
            selected_project,
        ),
        "workflow": _selected_ref(
            prepared.scope.workflow.to_data(),
            selected_workflow,
        ),
    }
    if prepared.tenant_source is not None and prepared.state.tenant_code is not None:
        resolved["tenant"] = {
            "value": prepared.state.tenant_code,
            "source": prepared.tenant_source,
        }
    if prepared.project_preference_used_fields:
        resolved["project_preference"] = {
            "used_fields": list(prepared.project_preference_used_fields)
        }
    return resolved


def _resolved_schedule(prepared: PreparedScheduleUpdate) -> JsonObject:
    schedule = prepared.current.schedule
    project = prepared.current.project
    resolved: JsonObject = {
        "id": schedule.id,
        "workflowDefinitionCode": schedule.workflow_native.value,
        "workflowDefinitionName": schedule.workflow_name,
        "projectName": schedule.project_name,
    }
    if "code" in project.to_data():
        resolved["projectCode"] = project.native.value
    else:
        resolved["projectId"] = project.native.value
    return resolved


def _mutation_result(
    schedule: ScheduleSnapshot,
    *,
    preview: SchedulePreviewData,
    resolved: JsonObject,
) -> CommandResult:
    warnings = confirmed_preview_warnings(preview)
    environment_warnings, environment_details = schedule_environment_warning(
        schedule.ds_version, schedule.environment_code
    )
    return CommandResult(
        data=schedule.to_data(),
        resolved=resolved,
        warnings=[*warnings, *environment_warnings],
        warning_details=[
            *confirmed_preview_warning_details(preview),
            *environment_details,
        ],
    )


def _with_environment_warning(
    result: CommandResult,
    schedule: ScheduleSnapshot,
) -> CommandResult:
    warnings, details = schedule_environment_warning(
        schedule.ds_version, schedule.environment_code
    )
    return replace(
        result,
        warnings=[*result.warnings, *warnings],
        warning_details=[*result.warning_details, *details],
    )


def _with_environment_warnings(
    result: CommandResult,
    schedules: Sequence[ScheduleSnapshot],
) -> CommandResult:
    for schedule in schedules:
        if schedule.environment_code is not None and schedule.environment_code > 0:
            return _with_environment_warning(result, schedule)
    return result


def _selected_ref(data: JsonObject, selected: SelectedValue) -> JsonObject:
    rendered = dict(data)
    rendered["source"] = selected.source
    return rendered


ResultT = TypeVar("ResultT")


def _schedule_call(
    operation: str,
    schedule_id: int,
    call: Callable[[], ResultT],
) -> ResultT:
    try:
        return call()
    except ApiResultError as error:
        raise translate_schedule_api_error(
            error,
            operation=operation,
            schedule_id=schedule_id,
        ) from error


__all__ = [
    "create_schedule_result",
    "delete_schedule_result",
    "explain_schedule_result",
    "get_schedule_result",
    "list_schedules_result",
    "offline_schedule_result",
    "online_schedule_result",
    "preview_schedule_result",
    "update_schedule_result",
]
