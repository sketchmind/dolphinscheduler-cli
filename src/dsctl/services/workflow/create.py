from __future__ import annotations

import shlex
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from dsctl.cli_surface import (
    WORKFLOW_RESOURCE,
)
from dsctl.errors import (
    ApiResultError,
    ConflictError,
    DsctlError,
    UserInputError,
)
from dsctl.models.workflow_spec import (
    WorkflowSpec,
    load_workflow_spec,
)
from dsctl.output import (
    CommandResult,
    dry_run_result,
    require_json_object,
)
from dsctl.services._data_quality_authoring import preflight_data_quality_authoring
from dsctl.services._dynamic_workflow_references import (
    resolve_dynamic_authoring_workflow_refs,
)
from dsctl.services._legacy_dependent_references import (
    resolve_legacy_authoring_dependent_refs,
)
from dsctl.services._legacy_workflow_references import (
    audit_legacy_authoring_workflow_graph,
    resolve_legacy_authoring_workflow_refs,
)
from dsctl.services._parameter_warnings import (
    ParameterWarningDetail,
    workflow_parameter_warnings,
)
from dsctl.services._schedule_environment_guard import (
    require_schedule_environment_inheritance,
)
from dsctl.services._schedule_support import (
    ScheduleCreateInput,
    confirmed_preview_warning_details,
    confirmed_preview_warnings,
    require_high_frequency_confirmation,
    schedule_confirmation_data,
    translate_schedule_api_error,
    validated_schedule_create_input,
)
from dsctl.services._task_datasource_refs import (
    resolve_workflow_spec_task_datasources,
)
from dsctl.services._task_resource_refs import (
    mr_resource_full_names_from_spec,
    resolve_task_resource_refs,
)
from dsctl.services._workflow import authoring
from dsctl.services._workflow.compile import (
    PreparedWorkflowCompilation,
    prepare_workflow_create_compilation,
)
from dsctl.services._workflow.render import (
    serialize_workflow as _serialize_workflow,
)
from dsctl.services._workflow.validation import (
    require_schedule_block_create_compatible,
)
from dsctl.services.runtime import (
    run_with_bound_domain_selection,
)
from dsctl.services.schedule_analysis import build_schedule_preview_data
from dsctl.services.task_authoring_catalog import TaskAuthoringIntent
from dsctl.services.version_resolution import resolve_runtime_selection
from dsctl.upstream.definition_models import (
    NativeCode,
    ProjectRef,
    WorkflowScope,
)
from dsctl.upstream.legacy_workflow_graph import (
    LegacyWorkflowGraphError,
    LegacyWorkflowGraphPayload,
    PreparedLegacyWorkflowGraph,
    prepare_legacy_workflow_graph,
)
from dsctl.upstream.protocol import ScheduleCreateSpec
from dsctl.upstream.schedules import ScheduleSnapshot, ScheduleState
from dsctl.upstream.workflows import WORKFLOW_DOMAIN

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.services._task_datasource_refs import TaskDatasourceResolutionData
    from dsctl.services.schedule_analysis import SchedulePreviewData
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog
    from dsctl.support.yaml_io import JsonObject, JsonValue
    from dsctl.upstream.protocol import (
        ScheduleCreateRequestPlan,
        ScheduleRecord,
    )
    from dsctl.upstream.task_parameter_projection import (
        TaskResourceRefIndex,
    )
    from dsctl.upstream.workflow_graph import (
        WorkflowCreatePayload,
    )
    from dsctl.upstream.workflows import (
        PreparedLegacyWorkflowCreate,
        PreparedWorkflowCreate,
    )

from dsctl.services.workflow._errors import (
    _raise_workflow_create_error,
    _raise_workflow_release_error,
)
from dsctl.services.workflow._graph import (
    _allocate_workflow_task_codes,
)
from dsctl.services.workflow._mutation import (
    _parameter_expression_warning_json_details,
    _prepare_workflow_definition_request,
    _require_workflow_wire_without_content,
    _workflow_wire_form,
    _workflow_wire_query,
    _workflow_wire_request_data,
)
from dsctl.services.workflow._outcomes import WorkflowMutationProgress
from dsctl.services.workflow._schedules import (
    _workflow_scope_data,
)
from dsctl.services.workflow._selection import (
    _identity_field,
    _project_selector,
    _require_new_workflow_name,
    _required_name,
    _resolve_create_project,
    _resolved_file_workflow_data,
)
from dsctl.services.workflow._types import (
    WorkflowCreateScheduleDryRunData,
    WorkflowServiceRuntime,
)
from dsctl.upstream.mutation_outcomes import verify_mutation


@dataclass(frozen=True)
class PreparedWorkflowSchedule:
    """One confirmed schedule intent and its resolved preflight state."""

    schedule_input: ScheduleCreateInput
    effective_state: ScheduleState
    preview: SchedulePreviewData


def create_workflow_result(
    *,
    file: Path,
    project: str | None = None,
    dry_run: bool = False,
    confirm_risk: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create one workflow definition from a YAML file."""
    selection = resolve_runtime_selection(env_file)
    profile = selection.execution_profile
    catalog = authoring.workflow_authoring_catalog_for_version(profile.ds_version)
    spec = _load_workflow_spec_or_error(file, catalog=catalog)
    return run_with_bound_domain_selection(
        selection,
        WORKFLOW_DOMAIN,
        _create_workflow_result,
        file=file,
        spec=spec,
        catalog=catalog,
        project=project,
        dry_run=dry_run,
        confirm_risk=confirm_risk,
    )


def _create_workflow_result(
    runtime: WorkflowServiceRuntime,
    *,
    file: Path,
    spec: WorkflowSpec,
    catalog: TaskAuthoringCatalog,
    project: str | None,
    dry_run: bool,
    confirm_risk: str | None,
) -> CommandResult:
    operations = runtime.domain.workflows
    operations.require_action("workflow.create")
    authoring.require_task_authoring_catalog_profile(
        catalog,
        ds_version=runtime.profile.ds_version,
    )
    project_ref, resolved_project_data = _resolve_create_project(
        runtime,
        project=project,
        spec=spec,
    )
    _require_new_workflow_name(
        runtime, project=project_ref, workflow_name=spec.workflow.name
    )
    spec, task_datasource_resolutions = resolve_workflow_spec_task_datasources(
        runtime.domain.task_datasources,
        spec,
        catalog=catalog,
        action="workflow.create",
    )
    prepared_schedule = _prepare_workflow_schedule_create(
        runtime,
        project=project_ref,
        spec=spec,
        confirm_risk=confirm_risk,
    )
    schedule_preview = None if prepared_schedule is None else prepared_schedule.preview
    preview_warnings = (
        [] if schedule_preview is None else confirmed_preview_warnings(schedule_preview)
    )
    try:
        legacy_resource_refs = (
            resolve_task_resource_refs(
                runtime.domain.task_resource_resolver,
                mr_resource_full_names_from_spec(
                    spec,
                    profile_version=runtime.profile.ds_version,
                ),
                boundary_resource=WORKFLOW_RESOURCE,
                action="workflow.create",
            )
            if runtime.domain.workflows.workflow_graph_family == "legacy-json"
            else None
        )
        compilation = prepare_creation(
            runtime,
            spec=spec,
            catalog=catalog,
            project=project_ref,
            resource_refs=legacy_resource_refs,
        )
    except UserInputError as error:
        file_arg = shlex.quote(str(file))
        raise UserInputError(
            error.message,
            details=error.details,
            suggestion=(
                f"Run `dsctl lint workflow {file_arg}`, fix task names and "
                "references, then retry "
                f"`dsctl workflow create --file {file_arg} --dry-run`."
            ),
        ) from error
    if isinstance(compilation, PreparedWorkflowCompilation):
        preflight_data_quality_authoring(
            runtime.domain.data_quality_authoring_inspector,
            spec=spec,
            projection_sources=compilation.projection_sources,
            ds_version=runtime.profile.ds_version,
            action="workflow.create",
        )
        workflow_refs = resolve_dynamic_authoring_workflow_refs(
            runtime.domain.workflows,
            project=project_ref,
            child_workflow_names=compilation.required_child_workflow_names,
            containing_workflow_code=None,
            audit_descendants=True,
            action="workflow.create",
        )
    else:
        workflow_refs = None
    resource_refs = resolve_task_resource_refs(
        runtime.domain.task_resource_resolver,
        compilation.required_resource_full_names
        if isinstance(compilation, PreparedWorkflowCompilation)
        else (),
        boundary_resource=WORKFLOW_RESOURCE,
        action="workflow.create",
    )
    parameter_warnings, parameter_warning_details = workflow_parameter_warnings(
        spec,
        parameter_semantics=catalog.parameter_semantics,
    )

    if dry_run:
        payload = (
            compilation.preview(
                resource_refs=resource_refs,
                workflow_refs=workflow_refs,
            )
            if isinstance(compilation, PreparedWorkflowCompilation)
            else compilation.preview()
        )
        prepared = _prepare_remote_workflow(
            runtime,
            project=project_ref,
            payload=payload,
            workflow_name=spec.workflow.name,
            workflow_description=spec.workflow.description,
        )
        return _workflow_create_dry_run_result(
            runtime,
            file=file,
            spec=spec,
            resolved_project_data=resolved_project_data,
            project=project_ref,
            prepared=prepared,
            schedule=prepared_schedule,
            task_datasource_resolutions=task_datasource_resolutions,
            parameter_warnings=parameter_warnings,
            parameter_warning_details=parameter_warning_details,
        )

    if isinstance(compilation, PreparedLegacyWorkflowGraph):
        payload = compilation.materialize()
    else:
        payload = compilation.materialize(
            _allocate_workflow_task_codes(
                runtime,
                project=project_ref,
                count=compilation.required_task_code_count,
                action="workflow.create",
            ),
            resource_refs=resource_refs,
            workflow_refs=workflow_refs,
        )

    progress = WorkflowMutationProgress(
        {"project": project_ref.to_data(), "workflow": {"name": spec.workflow.name}}
    )
    stage = "workflow_create"
    try:
        stage = "workflow_create"
        _create_remote_workflow(
            runtime,
            project=project_ref,
            payload=payload,
            workflow_name=spec.workflow.name,
            workflow_description=spec.workflow.description,
        )

        progress.complete(stage, mutation=True)
        stage = "workflow_identity_readback"
        created_scope = verify_mutation(
            lambda: operations.resolve_workflow(
                _project_selector(project_ref), spec.workflow.name
            ),
            ds_version=runtime.profile.ds_version,
            resource=WORKFLOW_RESOURCE,
            operation="create",
        )
        progress.complete(stage)
        progress.known_resources["workflow"] = created_scope.workflow.to_data()
        stage = "release_and_schedule"
        created_schedule = _apply_workflow_release_and_schedule(
            runtime,
            scope=created_scope,
            spec=spec,
            schedule=prepared_schedule,
            progress=progress,
        )

        stage = "workflow_readback"
        created_data: JsonObject
        if operations.workflow_graph_family == "legacy-json":
            created_scope = verify_mutation(
                lambda: operations.resolve_workflow(
                    _project_selector(project_ref), spec.workflow.name
                ),
                ds_version=runtime.profile.ds_version,
                resource=WORKFLOW_RESOURCE,
                operation="create",
            )
            created_data = _workflow_scope_data(
                created_scope,
                cast("ScheduleRecord | None", created_schedule),
            )
        else:
            payload_data = verify_mutation(
                lambda: operations.detail(created_scope, action="workflow.create"),
                ds_version=runtime.profile.ds_version,
                resource=WORKFLOW_RESOURCE,
                operation="create",
            )
            created_data = cast(
                "JsonObject",
                _serialize_workflow(
                    payload_data,
                    attached_schedule=cast(
                        "ScheduleRecord | None",
                        created_schedule,
                    ),
                ),
            )
        progress.complete(stage)
    except DsctlError as error:
        progress.annotate(error, failed_stage=stage)
        raise
    return CommandResult(
        data=require_json_object(
            created_data,
            label="workflow data",
        ),
        resolved={
            "project": resolved_project_data,
            "workflow": _resolved_file_workflow_data(created_scope.workflow),
            "mutation": progress.to_data(),
            "file": str(file),
            "task_datasources": cast("JsonValue", task_datasource_resolutions),
        },
        warnings=[
            *preview_warnings,
            *parameter_warnings,
        ],
        warning_details=(
            []
            if schedule_preview is None
            else [
                require_json_object(detail, label="workflow create warning detail")
                for detail in confirmed_preview_warning_details(schedule_preview)
            ]
        )
        + _parameter_expression_warning_json_details(parameter_warning_details),
    )


def prepare_creation(
    runtime: WorkflowServiceRuntime,
    *,
    spec: WorkflowSpec,
    catalog: TaskAuthoringCatalog,
    project: ProjectRef | None = None,
    resource_refs: TaskResourceRefIndex | None = None,
) -> PreparedWorkflowCompilation[WorkflowCreatePayload] | PreparedLegacyWorkflowGraph:
    """Select the exact hidden graph compiler without changing YAML intent."""
    if runtime.domain.workflows.workflow_graph_family != "legacy-json":
        return prepare_workflow_create_compilation(spec, catalog=catalog)
    if project is None:
        try:
            return prepare_legacy_workflow_graph(spec, resource_refs=resource_refs)
        except LegacyWorkflowGraphError as error:
            raise UserInputError(
                str(error),
                suggestion=(
                    "Fix legacy task names and graph references, then retry the "
                    "workflow dry run."
                ),
            ) from error
    try:
        dependent_refs = resolve_legacy_authoring_dependent_refs(
            runtime.domain.workflows,
            spec=spec,
            action="workflow.create",
        )
        resolution = resolve_legacy_authoring_workflow_refs(
            runtime.domain.workflows,
            project=project,
            spec=spec,
            containing_workflow_id=None,
            action="workflow.create",
        )
        legacy_compilation = prepare_legacy_workflow_graph(
            spec,
            workflow_refs=resolution.refs,
            dependent_refs=dependent_refs,
            resource_refs=resource_refs,
        )
        audit_legacy_authoring_workflow_graph(
            runtime.domain.workflows,
            project=project,
            compilation=legacy_compilation,
            containing_workflow_id=None,
            resolution=resolution,
            action="workflow.create",
        )
    except LegacyWorkflowGraphError as error:
        raise UserInputError(
            str(error),
            suggestion=(
                "Fix legacy task names and graph references, then retry the "
                "workflow dry run."
            ),
        ) from error
    return legacy_compilation


def _load_workflow_spec_or_error(
    path: Path,
    *,
    catalog: TaskAuthoringCatalog,
) -> WorkflowSpec:
    try:
        return load_workflow_spec(
            path,
            authoring_context=authoring.workflow_authoring_context(
                catalog=catalog,
                intent=TaskAuthoringIntent.TYPED_CREATE,
            ),
        )
    except (TypeError, ValueError) as exc:
        raise UserInputError(
            str(exc),
            details={"file": str(path)},
            suggestion=(
                "Run `dsctl template workflow` to inspect the stable YAML surface, "
                "then run `dsctl lint workflow PATH` before retrying create."
            ),
        ) from exc


def _prepare_workflow_schedule_create(
    runtime: WorkflowServiceRuntime,
    *,
    project: ProjectRef,
    spec: WorkflowSpec,
    confirm_risk: str | None,
) -> PreparedWorkflowSchedule | None:
    schedule_input = _workflow_schedule_input(spec)
    if schedule_input is None:
        return None
    require_schedule_block_create_compatible(spec)
    schedule_state = _workflow_schedule_state(schedule_input)
    effective_schedule_state = runtime.domain.schedules.effective_create_state(
        project, schedule_state
    )
    require_schedule_environment_inheritance(
        runtime.profile.ds_version, effective_schedule_state.environment_code
    )
    preview_result = runtime.domain.schedules.preview(
        _project_selector(project),
        effective_schedule_state,
    )
    preview = build_schedule_preview_data(list(preview_result.times))
    confirmation_payload = _workflow_schedule_create_confirmation_payload(
        project=project,
        workflow_name=spec.workflow.name,
        effective_state=effective_schedule_state,
    )
    require_high_frequency_confirmation(
        action="workflow.create",
        confirmation=confirm_risk,
        preview=preview,
        schedule_payload=confirmation_payload,
    )
    return PreparedWorkflowSchedule(
        schedule_input=schedule_input,
        effective_state=effective_schedule_state,
        preview=preview,
    )


def _workflow_schedule_state(schedule_input: ScheduleCreateInput) -> ScheduleState:
    return ScheduleState(
        crontab=schedule_input["crontab"],
        start_time=schedule_input["start_time"],
        end_time=schedule_input["end_time"],
        timezone_id=schedule_input["timezone_id"],
        failure_strategy=schedule_input["failure_strategy"] or "CONTINUE",
        warning_type=schedule_input["warning_type"] or "NONE",
        warning_group_id=schedule_input["warning_group_id"],
        workflow_instance_priority=(
            schedule_input["workflow_instance_priority"] or "MEDIUM"
        ),
        worker_group=schedule_input["worker_group"] or "default",
        tenant_code=schedule_input["tenant_code"],
        environment_code=schedule_input["environment_code"],
        missed_fire_policy=schedule_input.get("missed_fire_policy"),
    )


def _workflow_schedule_input(spec: WorkflowSpec) -> ScheduleCreateInput | None:
    schedule = spec.schedule
    if schedule is None:
        return None
    return validated_schedule_create_input(
        cron=schedule.cron,
        start=schedule.start,
        end=schedule.end,
        timezone=schedule.timezone,
        failure_strategy=(
            None
            if schedule.failure_strategy is None
            else schedule.failure_strategy.value
        ),
        warning_type=None,
        warning_group_id=0,
        priority=None if schedule.priority is None else schedule.priority.value,
        worker_group=None,
        tenant_code=None,
        environment_code=None,
        missed_fire_policy=schedule.missed_fire_policy,
    )


def _workflow_create_dry_run_requests(
    runtime: WorkflowServiceRuntime,
    *,
    project: ProjectRef,
    workflow_name: str,
    prepared: PreparedWorkflowCreate | PreparedLegacyWorkflowCreate,
    workflow_should_online: bool,
    schedule: PreparedWorkflowSchedule | None,
    schedule_should_online: bool,
) -> list[JsonObject]:
    requests = [
        _workflow_wire_request_data(
            prepared.request,
            label="workflow create request",
        )
    ]
    legacy = runtime.domain.workflows.workflow_graph_family == "legacy-json"
    workflow_identity = "id" if legacy else "code"
    created_workflow_native = f"<{workflow_name}:created_workflow_{workflow_identity}>"
    if workflow_should_online:
        release = runtime.domain.workflows.prepare_release(
            project,
            workflow_code=created_workflow_native,
            state="ONLINE",
        )
        requests.append(
            _workflow_wire_request_data(
                release.request,
                label="workflow release request",
            )
        )
    if schedule is None:
        return requests
    project_native = project.native.value
    created_schedule_id = f"<{workflow_name}:created_schedule_id>"
    state = schedule.effective_state
    schedule_request: ScheduleCreateRequestPlan = runtime.domain.schedules.plan_create(
        spec=ScheduleCreateSpec(
            project_code=project_native,
            project_name=(
                _required_name(project.name, label="project") if legacy else None
            ),
            workflow_code=created_workflow_native,
            crontab=state.crontab,
            start_time=state.start_time,
            end_time=state.end_time,
            timezone_id=state.timezone_id,
            failure_strategy=state.failure_strategy,
            warning_type=state.warning_type,
            warning_group_id=state.warning_group_id,
            workflow_instance_priority=state.workflow_instance_priority,
            worker_group=state.worker_group,
            tenant_code=state.tenant_code,
            environment_code=state.environment_code,
            missed_fire_policy=state.missed_fire_policy,
        )
    )
    requests.append(
        require_json_object(
            schedule_request,
            label="schedule create dry-run request",
        )
    )
    if schedule_should_online:
        requests.append(
            require_json_object(
                runtime.domain.schedules.plan_release(
                    project,
                    schedule_id=created_schedule_id,
                    state="ONLINE",
                ),
                label="schedule online dry-run request",
            )
        )
    return requests


def _workflow_create_dry_run_result(
    runtime: WorkflowServiceRuntime,
    *,
    file: Path,
    spec: WorkflowSpec,
    resolved_project_data: dict[str, int | str | None],
    project: ProjectRef,
    prepared: PreparedWorkflowCreate | PreparedLegacyWorkflowCreate,
    schedule: PreparedWorkflowSchedule | None,
    task_datasource_resolutions: list[TaskDatasourceResolutionData],
    parameter_warnings: list[str],
    parameter_warning_details: list[ParameterWarningDetail],
) -> CommandResult:
    request = prepared.request
    _require_workflow_wire_without_content(
        request,
        label="workflow create request",
    )
    schedule_data = None
    if schedule is not None:
        schedule_data = _workflow_create_dry_run_schedule_data(
            project=project,
            workflow_name=spec.workflow.name,
            schedule=schedule,
        )
    return dry_run_result(
        method=request.method,
        path=request.path,
        params=_workflow_wire_query(request, label="workflow create query"),
        json_body=request.json,
        form_data=_workflow_wire_form(request, label="workflow create form"),
        requests=_workflow_create_dry_run_requests(
            runtime,
            project=project,
            workflow_name=spec.workflow.name,
            prepared=prepared,
            schedule=schedule,
            schedule_should_online=(
                spec.schedule is not None
                and spec.schedule.desired_release_state().value == "ONLINE"
            ),
            workflow_should_online=spec.workflow.release_state.value == "ONLINE",
        ),
        resolved={
            "project": resolved_project_data,
            "workflow": {
                "name": spec.workflow.name,
                "source": "file",
            },
            "file": str(file),
            "task_datasources": cast("JsonValue", task_datasource_resolutions),
        },
        extra_data={
            **({} if schedule_data is None else schedule_data),
            "changes": {
                "workflow_name": spec.workflow.name,
                "release_state": spec.workflow.release_state.value,
                "task_count": len(spec.tasks),
                "task_names": [task.name for task in spec.tasks],
            },
        },
        warnings=parameter_warnings,
        warning_details=_parameter_expression_warning_json_details(
            parameter_warning_details
        ),
    )


def _workflow_schedule_create_confirmation_payload(
    *,
    project: ProjectRef,
    workflow_name: str,
    effective_state: ScheduleState,
) -> JsonObject:
    return require_json_object(
        {
            f"project_{_identity_field(project)}": project.native.value,
            "project_name": project.name,
            "workflow_name": workflow_name,
            "schedule": effective_state.to_data(),
        },
        label="workflow schedule confirmation payload",
    )


def _workflow_create_dry_run_schedule_data(
    *,
    project: ProjectRef,
    workflow_name: str,
    schedule: PreparedWorkflowSchedule,
) -> JsonObject:
    return require_json_object(
        WorkflowCreateScheduleDryRunData(
            schedule_preview=schedule.preview,
            schedule_confirmation=schedule_confirmation_data(
                action="workflow.create",
                preview=schedule.preview,
                schedule_payload=_workflow_schedule_create_confirmation_payload(
                    project=project,
                    workflow_name=workflow_name,
                    effective_state=schedule.effective_state,
                ),
            ),
        ),
        label="workflow create dry-run schedule data",
    )


def _prepare_remote_workflow(
    runtime: WorkflowServiceRuntime,
    *,
    project: ProjectRef,
    payload: WorkflowCreatePayload | LegacyWorkflowGraphPayload,
    workflow_name: str,
    workflow_description: str | None,
) -> PreparedWorkflowCreate | PreparedLegacyWorkflowCreate:
    return _prepare_workflow_definition_request(
        prepare=lambda: runtime.domain.prepare_definition_create(
            project,
            payload=payload,
            name=workflow_name,
            description=workflow_description,
        ),
        translate_error=lambda error: _raise_workflow_create_error(
            error, project=project, workflow_name=workflow_name
        ),
    )


def _create_remote_workflow(
    runtime: WorkflowServiceRuntime,
    *,
    project: ProjectRef,
    payload: WorkflowCreatePayload | LegacyWorkflowGraphPayload,
    workflow_name: str,
    workflow_description: str | None,
) -> None:
    prepared = _prepare_remote_workflow(
        runtime,
        project=project,
        payload=payload,
        workflow_name=workflow_name,
        workflow_description=workflow_description,
    )
    try:
        runtime.domain.workflows.apply_create(prepared)
    except ApiResultError as exc:
        _raise_workflow_create_error(
            exc,
            project=project,
            workflow_name=workflow_name,
        )


def _apply_workflow_release_and_schedule(
    runtime: WorkflowServiceRuntime,
    *,
    scope: WorkflowScope,
    spec: WorkflowSpec,
    schedule: PreparedWorkflowSchedule | None,
    progress: WorkflowMutationProgress,
) -> ScheduleSnapshot | None:
    stage = "workflow_online"
    try:
        stage = "workflow_online"
        if spec.workflow.release_state.value == "ONLINE":
            try:
                runtime.domain.workflows.release(
                    scope,
                    state="ONLINE",
                )
            except ApiResultError as exc:
                _raise_workflow_release_error(
                    exc,
                    project=scope.project,
                    workflow=scope.workflow,
                    action="online",
                )
            progress.complete(stage, mutation=True)
        if schedule is None:
            return None
        stage = "schedule_create"
        created_schedule = _create_workflow_schedule(
            runtime,
            scope=scope,
            schedule=schedule,
        )
        progress.complete(stage, mutation=True)
        progress.known_resources["schedule"] = {"id": created_schedule.id}
        if (
            spec.schedule is None
            or spec.schedule.desired_release_state().value != "ONLINE"
        ):
            return created_schedule
        stage = "schedule_online"
        created_schedule_id = created_schedule.id
        try:
            online_schedule = runtime.domain.schedules.online(created_schedule_id)
        except ApiResultError as exc:
            raise translate_schedule_api_error(
                exc,
                operation="online",
                schedule_id=created_schedule_id,
                workflow_code=(
                    scope.workflow.native.value
                    if isinstance(scope.workflow.native, NativeCode)
                    else None
                ),
                workflow_name=scope.workflow.name,
            ) from exc
        progress.complete(stage, mutation=True)
    except DsctlError as error:
        progress.annotate(error, failed_stage=stage)
        raise
    else:
        return online_schedule


def _create_workflow_schedule(
    runtime: WorkflowServiceRuntime,
    *,
    scope: WorkflowScope,
    schedule: PreparedWorkflowSchedule,
) -> ScheduleSnapshot:
    state = _workflow_schedule_state(schedule.schedule_input)
    try:
        prepared = runtime.domain.schedules.prepare_create(
            _project_selector(scope.project),
            scope.workflow.name or str(scope.workflow.native.value),
            state,
        )
        require_schedule_environment_inheritance(
            runtime.profile.ds_version, prepared.state.environment_code
        )
        expected = schedule.effective_state.to_data()
        actual = prepared.state.to_data()
        if actual != expected:
            changed_fields = sorted(
                field
                for field in expected.keys() | actual.keys()
                if expected.get(field) != actual.get(field)
            )
            message = (
                "Effective schedule settings changed during workflow creation; "
                "the schedule was not created"
            )
            raise ConflictError(
                message,
                details={"changed_fields": changed_fields},
                suggestion=(
                    "Inspect the created workflow and current project preferences "
                    "before creating its schedule."
                ),
            )
        created_schedule = runtime.domain.schedules.create(prepared)
    except ApiResultError as exc:
        raise translate_schedule_api_error(
            exc,
            operation="create",
            workflow_code=scope.workflow.native.value,
            workflow_name=scope.workflow.name,
            environment_code=schedule.schedule_input["environment_code"],
        ) from exc
    return created_schedule
