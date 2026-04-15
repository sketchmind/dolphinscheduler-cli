from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal, TypedDict, cast

from dsctl.cli_surface import SCHEDULE_RESOURCE, TASK_RESOURCE, WORKFLOW_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ConflictError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.models.workflow_spec import (
    WorkflowSpec,
    load_workflow_spec,
)
from dsctl.output import (
    CommandResult,
    build_dry_run_request,
    dry_run_result,
    require_json_object,
    require_json_value,
)
from dsctl.services._parameter_warnings import (
    ParameterExpressionWarningDetail,
    workflow_parameter_expression_warnings,
)
from dsctl.services._runtime_defaults import (
    ProjectPreferenceDefaults,
    load_project_preference_defaults,
    select_worker_group,
)
from dsctl.services._schedule_support import (
    ScheduleConfirmationData,
    ScheduleCreateInput,
    confirmed_preview_warning_details,
    confirmed_preview_warnings,
    preview_schedule,
    require_high_frequency_confirmation,
    schedule_confirmation_data,
    translate_schedule_api_error,
    validated_schedule_create_input,
)
from dsctl.services._serialization import (
    enum_value,
    optional_text,
    require_resource_int,
)
from dsctl.services._validation import (
    require_delete_force,
    require_non_empty_text,
    require_non_negative_int,
)
from dsctl.services._workflow_compile import (
    WorkflowCreatePayload,
    WorkflowUpdatePayload,
)
from dsctl.services._workflow_compile import (
    compile_workflow_create_payload as _compile_workflow_create_payload,
)
from dsctl.services._workflow_digest import (
    digest_workflow as _digest_workflow,
)
from dsctl.services._workflow_mutation import (
    compile_workflow_mutation_plan,
    load_workflow_patch_or_error,
)
from dsctl.services._workflow_render import (
    WorkflowData,
    WorkflowListItem,
)
from dsctl.services._workflow_render import (
    serialize_workflow as _serialize_workflow,
)
from dsctl.services._workflow_render import (
    serialize_workflow_dag as _serialize_workflow_dag,
)
from dsctl.services._workflow_render import (
    serialize_workflow_ref as _serialize_workflow_ref,
)
from dsctl.services._workflow_render import (
    workflow_yaml_document as _workflow_yaml_document,
)
from dsctl.services._workflow_validation import (
    require_schedule_block_create_compatible,
)
from dsctl.services.resolver import ResolvedProject, ResolvedTask, ResolvedWorkflow
from dsctl.services.resolver import project as resolve_project
from dsctl.services.resolver import task as resolve_task
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
    from pathlib import Path

    from dsctl.models.workflow_patch import WorkflowPatchSpec
    from dsctl.services._workflow_patch import WorkflowPatchDiffData
    from dsctl.services.schedule_analysis import SchedulePreviewData
    from dsctl.services.selection import SelectionData
    from dsctl.support.yaml_io import JsonObject
    from dsctl.upstream.protocol import (
        WorkflowPayloadRecord,
    )

WorkflowOutputFormat = Literal["json", "yaml"]
WorkflowRunTaskScope = Literal["self", "pre", "post"]
WorkflowBackfillTimeMode = Literal["range", "dates"]
INTERNAL_SERVER_ERROR_ARGS = 10000
WORKFLOW_DEFINITION_NOT_EXIST = 50003
WORKFLOW_DEFINITION_NOT_RELEASE = 50004
WORKFLOW_DEFINITION_NOT_ALLOWED_EDIT = 50008
START_WORKFLOW_INSTANCE_ERROR = 50014
PROJECT_NOT_FOUND = 10018
WORKFLOW_DEFINITION_NAME_EXIST = 10168
TASK_NAME_DUPLICATE_ERROR = 10198
DATA_IS_NOT_VALID = 50017
WORKFLOW_NODE_HAS_CYCLE = 50019
WORKFLOW_NODE_S_PARAMETER_INVALID = 50020
TASK_DEFINE_NOT_EXIST = 50030
CHECK_WORKFLOW_TASK_RELATION_ERROR = 50036
_WORKFLOW_CREATE_REVIEW_SUGGESTION = (
    "Run `dsctl lint workflow FILE` and `dsctl workflow create --file FILE "
    "--dry-run` to inspect the workflow spec and compiled DS-native payload "
    "before retrying."
)
_WORKFLOW_EDIT_DRY_RUN_SUGGESTION = (
    "Retry with `dsctl workflow edit --dry-run` to inspect the compiled diff "
    "and DS-native payload before sending it again."
)
_WORKFLOW_RUN_FAILURE_STRATEGIES = ("CONTINUE", "END")
_WORKFLOW_RUN_WARNING_TYPES = ("NONE", "SUCCESS", "FAILURE", "ALL")
_WORKFLOW_RUN_PRIORITIES = ("HIGHEST", "HIGH", "MEDIUM", "LOW", "LOWEST")
_WORKFLOW_BACKFILL_RUN_MODES = ("RUN_MODE_SERIAL", "RUN_MODE_PARALLEL")
_WORKFLOW_BACKFILL_COMPLEMENT_DEPENDENT_MODES = ("OFF_MODE", "ALL_DEPENDENT")
_WORKFLOW_BACKFILL_EXECUTION_ORDERS = ("DESC_ORDER", "ASC_ORDER")


class WorkflowYamlExportData(TypedDict):
    """YAML export payload kept inside the standard JSON envelope."""

    yaml: str


class WorkflowRunData(TypedDict):
    """Stable payload emitted for one workflow run request."""

    workflowInstanceIds: list[int]


class WorkflowRunTaskDependencyWarningDetail(TypedDict):
    """Structured warning for task-scoped workflow starts."""

    code: str
    message: str
    blocking: bool
    scope: str
    dependent_resolution: str


class WorkflowExecutionDryRunWarningDetail(TypedDict):
    """Structured warning for DS server-side workflow execution dry-run."""

    code: str
    message: str
    blocking: bool
    request_sent: bool


class DeleteWorkflowData(TypedDict):
    """Stable payload emitted for one workflow deletion."""

    deleted: bool
    workflow: WorkflowData


class WorkflowCreateScheduleDryRunData(TypedDict):
    """Dry-run metadata for one inline schedule block."""

    schedule_preview: SchedulePreviewData
    schedule_confirmation: ScheduleConfirmationData


class WorkflowEditConstraintData(TypedDict):
    """Structured workflow edit precondition or side-effect."""

    code: str
    message: str
    blocking: bool
    current_release_state: str | None
    required_release_state: str | None
    current_schedule_release_state: str | None


class WorkflowEditScheduleImpactData(TypedDict):
    """Structured schedule-impact note for workflow edit."""

    code: str
    message: str
    desired_workflow_release_state: str | None
    current_schedule_release_state: str | None


class WorkflowReleaseWarningDetail(TypedDict):
    """Structured warning emitted by workflow release actions."""

    code: str
    message: str
    action: Literal["online", "offline"]
    workflow_release_state: str | None
    schedule_release_state: str | None


class WorkflowEditNoChangeWarningDetail(TypedDict):
    """Structured warning emitted when one workflow patch changes nothing."""

    code: str
    message: str
    no_change: bool
    request_sent: bool


@dataclass(frozen=True)
class _ResolvedWorkflowTarget:
    """One fully resolved project/workflow target for existing workflow actions."""

    selected_project: SelectedValue
    resolved_project: ResolvedProject
    selected_workflow: SelectedValue
    resolved_workflow: ResolvedWorkflow


@dataclass(frozen=True)
class _SelectedOptionalInt:
    """One optional integer runtime input plus the source that supplied it."""

    value: int | None
    source: str


@dataclass(frozen=True)
class _WorkflowRunSettings:
    """Normalized DS start-workflow-instance settings."""

    worker_group: SelectedValue
    tenant: SelectedValue
    failure_strategy: SelectedValue
    warning_type: SelectedValue
    workflow_instance_priority: SelectedValue
    warning_group_id: _SelectedOptionalInt
    environment_code: _SelectedOptionalInt
    start_params: str | None
    start_param_names: list[str]
    execution_dry_run: bool


@dataclass(frozen=True)
class _WorkflowBackfillSettings:
    """Normalized DS complement-data settings."""

    schedule_time: str
    schedule_time_mode: WorkflowBackfillTimeMode
    run_mode: SelectedValue
    expected_parallelism_number: int
    complement_dependent_mode: SelectedValue
    all_level_dependent: bool
    execution_order: SelectedValue


def list_workflows_result(
    *,
    project: str | None = None,
    search: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """List workflows inside one resolved project."""
    normalized_search = optional_text(search)
    return run_with_service_runtime(
        env_file,
        _list_workflows_result,
        project=project,
        search=normalized_search,
    )


def get_workflow_result(
    workflow: str | None,
    *,
    project: str | None = None,
    output_format: str = "json",
    env_file: str | None = None,
) -> CommandResult:
    """Get one workflow by context-aware name or code."""
    normalized_output_format = _normalized_output_format(output_format)
    return run_with_service_runtime(
        env_file,
        _get_workflow_result,
        workflow=workflow,
        project=project,
        output_format=normalized_output_format,
    )


def describe_workflow_result(
    workflow: str | None,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Describe one workflow with tasks and task relations."""
    return run_with_service_runtime(
        env_file,
        _describe_workflow_result,
        workflow=workflow,
        project=project,
    )


def digest_workflow_result(
    workflow: str | None,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Return one compact workflow graph summary."""
    return run_with_service_runtime(
        env_file,
        _digest_workflow_result,
        workflow=workflow,
        project=project,
    )


def create_workflow_result(
    *,
    file: Path,
    project: str | None = None,
    dry_run: bool = False,
    confirm_risk: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create one workflow definition from a YAML file."""
    spec = _load_workflow_spec_or_error(file)
    return run_with_service_runtime(
        env_file,
        _create_workflow_result,
        file=file,
        spec=spec,
        project=project,
        dry_run=dry_run,
        confirm_risk=confirm_risk,
    )


def edit_workflow_result(
    workflow: str | None,
    *,
    patch: Path,
    project: str | None = None,
    dry_run: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """Edit one workflow definition from a YAML patch file."""
    workflow_patch = load_workflow_patch_or_error(patch)
    return run_with_service_runtime(
        env_file,
        _edit_workflow_result,
        workflow=workflow,
        patch_file=patch,
        patch=workflow_patch,
        project=project,
        dry_run=dry_run,
    )


def run_workflow_result(
    workflow: str | None,
    *,
    project: str | None = None,
    worker_group: str | None = None,
    tenant: str | None = None,
    failure_strategy: str | None = None,
    priority: str | None = None,
    warning_type: str | None = None,
    warning_group_id: int | None = None,
    environment_code: int | None = None,
    params: list[str] | None = None,
    dry_run: bool = False,
    execution_dry_run: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """Trigger one workflow definition and return created workflow instance ids."""
    normalized_worker_group = optional_text(worker_group)
    normalized_tenant = optional_text(tenant)
    return run_with_service_runtime(
        env_file,
        _run_workflow_result,
        workflow=workflow,
        project=project,
        worker_group=normalized_worker_group,
        tenant=normalized_tenant,
        failure_strategy=failure_strategy,
        priority=priority,
        warning_type=warning_type,
        warning_group_id=warning_group_id,
        environment_code=environment_code,
        params=[] if params is None else params,
        dry_run=dry_run,
        execution_dry_run=execution_dry_run,
    )


def run_workflow_task_result(
    workflow: str | None,
    *,
    task: str,
    project: str | None = None,
    scope: str = "self",
    worker_group: str | None = None,
    tenant: str | None = None,
    failure_strategy: str | None = None,
    priority: str | None = None,
    warning_type: str | None = None,
    warning_group_id: int | None = None,
    environment_code: int | None = None,
    params: list[str] | None = None,
    dry_run: bool = False,
    execution_dry_run: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """Start one workflow definition from a selected task."""
    normalized_task = optional_text(task)
    if normalized_task is None:
        message = "Task name or code is required"
        raise UserInputError(
            message,
            details={"resource": TASK_RESOURCE},
            suggestion="Pass `--task TASK`.",
        )
    normalized_scope = _normalized_task_run_scope(scope)
    normalized_worker_group = optional_text(worker_group)
    normalized_tenant = optional_text(tenant)
    return run_with_service_runtime(
        env_file,
        _run_workflow_task_result,
        workflow=workflow,
        task=normalized_task,
        project=project,
        scope=normalized_scope,
        worker_group=normalized_worker_group,
        tenant=normalized_tenant,
        failure_strategy=failure_strategy,
        priority=priority,
        warning_type=warning_type,
        warning_group_id=warning_group_id,
        environment_code=environment_code,
        params=[] if params is None else params,
        dry_run=dry_run,
        execution_dry_run=execution_dry_run,
    )


def backfill_workflow_result(
    workflow: str | None,
    *,
    project: str | None = None,
    start: str | None = None,
    end: str | None = None,
    dates: list[str] | None = None,
    task: str | None = None,
    scope: str = "self",
    run_mode: str | None = None,
    expected_parallelism_number: int = 2,
    complement_dependent_mode: str | None = None,
    all_level_dependent: bool = False,
    execution_order: str | None = None,
    worker_group: str | None = None,
    tenant: str | None = None,
    failure_strategy: str | None = None,
    priority: str | None = None,
    warning_type: str | None = None,
    warning_group_id: int | None = None,
    environment_code: int | None = None,
    params: list[str] | None = None,
    dry_run: bool = False,
    execution_dry_run: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """Backfill one workflow definition and return created workflow instance ids."""
    normalized_task = optional_text(task)
    normalized_scope = _normalized_task_run_scope(scope)
    normalized_worker_group = optional_text(worker_group)
    normalized_tenant = optional_text(tenant)
    return run_with_service_runtime(
        env_file,
        _backfill_workflow_result,
        workflow=workflow,
        project=project,
        start=start,
        end=end,
        dates=[] if dates is None else dates,
        task=normalized_task,
        scope=normalized_scope,
        run_mode=run_mode,
        expected_parallelism_number=expected_parallelism_number,
        complement_dependent_mode=complement_dependent_mode,
        all_level_dependent=all_level_dependent,
        execution_order=execution_order,
        worker_group=normalized_worker_group,
        tenant=normalized_tenant,
        failure_strategy=failure_strategy,
        priority=priority,
        warning_type=warning_type,
        warning_group_id=warning_group_id,
        environment_code=environment_code,
        params=[] if params is None else params,
        dry_run=dry_run,
        execution_dry_run=execution_dry_run,
    )


def delete_workflow_result(
    workflow: str | None,
    *,
    project: str | None = None,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one workflow definition after explicit confirmation."""
    require_delete_force(force=force, resource_label="Workflow")
    return run_with_service_runtime(
        env_file,
        _delete_workflow_result,
        workflow=workflow,
        project=project,
    )


def online_workflow_result(
    workflow: str | None,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Bring one workflow definition online and return the refreshed payload."""
    return run_with_service_runtime(
        env_file,
        _set_workflow_release_state_result,
        workflow=workflow,
        project=project,
        action="online",
    )


def offline_workflow_result(
    workflow: str | None,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Bring one workflow definition offline and return the refreshed payload."""
    return run_with_service_runtime(
        env_file,
        _set_workflow_release_state_result,
        workflow=workflow,
        project=project,
        action="offline",
    )


def _list_workflows_result(
    runtime: ServiceRuntime,
    *,
    project: str | None,
    search: str | None,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    workflows = runtime.upstream.workflows.list(project_code=resolved_project.code)

    items: list[WorkflowListItem] = [
        _serialize_workflow_ref(workflow)
        for workflow in workflows
        if search is None
        or (workflow.name is not None and search.lower() in workflow.name.lower())
    ]
    return CommandResult(
        data=require_json_value(items, label="workflow list data"),
        resolved={
            "project": _resolved_project_selection(
                resolved_project,
                selected_project,
            ),
            "search": search,
        },
    )


def _get_workflow_result(
    runtime: ServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
    output_format: WorkflowOutputFormat,
) -> CommandResult:
    target = _resolve_workflow_target(
        runtime,
        workflow=workflow,
        project=project,
    )
    if output_format == "yaml":
        dag = runtime.upstream.workflows.describe(
            project_code=target.resolved_project.code,
            code=target.resolved_workflow.code,
        )
        data = require_json_object(
            WorkflowYamlExportData(
                yaml=_workflow_yaml_document(
                    dag,
                    project=target.resolved_project,
                )
            ),
            label="workflow yaml export",
        )
    else:
        payload = runtime.upstream.workflows.get(code=target.resolved_workflow.code)
        data = require_json_object(
            _serialize_workflow(payload),
            label="workflow data",
        )

    return CommandResult(
        data=data,
        resolved={
            "project": _resolved_project_selection(
                target.resolved_project,
                target.selected_project,
            ),
            "workflow": _resolved_workflow_selection(
                target.resolved_workflow,
                target.selected_workflow,
            ),
            "format": output_format,
        },
    )


def _describe_workflow_result(
    runtime: ServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
) -> CommandResult:
    target = _resolve_workflow_target(
        runtime,
        workflow=workflow,
        project=project,
    )
    dag = runtime.upstream.workflows.describe(
        project_code=target.resolved_project.code,
        code=target.resolved_workflow.code,
    )
    data = require_json_object(
        _serialize_workflow_dag(dag),
        label="workflow describe data",
    )

    return CommandResult(
        data=data,
        resolved={
            "project": _resolved_project_selection(
                target.resolved_project,
                target.selected_project,
            ),
            "workflow": _resolved_workflow_selection(
                target.resolved_workflow,
                target.selected_workflow,
            ),
        },
    )


def _digest_workflow_result(
    runtime: ServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
) -> CommandResult:
    target = _resolve_workflow_target(
        runtime,
        workflow=workflow,
        project=project,
    )
    dag = runtime.upstream.workflows.describe(
        project_code=target.resolved_project.code,
        code=target.resolved_workflow.code,
    )
    data = require_json_object(
        _digest_workflow(_serialize_workflow_dag(dag)),
        label="workflow digest data",
    )
    return CommandResult(
        data=data,
        resolved={
            "project": _resolved_project_selection(
                target.resolved_project,
                target.selected_project,
            ),
            "workflow": _resolved_workflow_selection(
                target.resolved_workflow,
                target.selected_workflow,
            ),
        },
    )


def _create_workflow_result(
    runtime: ServiceRuntime,
    *,
    file: Path,
    spec: WorkflowSpec,
    project: str | None,
    dry_run: bool,
    confirm_risk: str | None,
) -> CommandResult:
    resolved_project, resolved_project_data = _resolve_create_project(
        runtime,
        project=project,
        spec=spec,
    )
    schedule_input, schedule_preview, preview_warnings = (
        _prepare_workflow_schedule_create(
            runtime,
            project_code=resolved_project.code,
            spec=spec,
            confirm_risk=confirm_risk,
        )
    )
    payload = _compile_workflow_create_payload(spec)
    parameter_warnings, parameter_warning_details = (
        workflow_parameter_expression_warnings(spec)
    )

    if dry_run:
        return _workflow_create_dry_run_result(
            file=file,
            spec=spec,
            resolved_project_data=resolved_project_data,
            project_code=resolved_project.code,
            workflow_payload=payload,
            schedule_input=schedule_input,
            schedule_preview=schedule_preview,
            parameter_warnings=parameter_warnings,
            parameter_warning_details=parameter_warning_details,
        )

    _create_remote_workflow(
        runtime,
        project_code=resolved_project.code,
        payload=payload,
        project=resolved_project,
        workflow_name=spec.workflow.name,
    )

    resolved_workflow = resolve_workflow(
        spec.workflow.name,
        adapter=runtime.upstream.workflows,
        project_code=resolved_project.code,
    )
    _apply_workflow_release_and_schedule(
        runtime,
        project_code=resolved_project.code,
        resolved_workflow=resolved_workflow,
        spec=spec,
        schedule_input=schedule_input,
    )

    payload_data = runtime.upstream.workflows.get(code=resolved_workflow.code)
    return CommandResult(
        data=require_json_object(
            _serialize_workflow(payload_data),
            label="workflow data",
        ),
        resolved={
            "project": resolved_project_data,
            "workflow": _resolved_file_workflow_data(resolved_workflow),
            "file": str(file),
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


def _edit_workflow_result(
    runtime: ServiceRuntime,
    *,
    workflow: str | None,
    patch_file: Path,
    patch: WorkflowPatchSpec,
    project: str | None,
    dry_run: bool,
) -> CommandResult:
    target = _resolve_workflow_target(
        runtime,
        workflow=workflow,
        project=project,
    )
    dag = runtime.upstream.workflows.describe(
        project_code=target.resolved_project.code,
        code=target.resolved_workflow.code,
    )
    live_payload = runtime.upstream.workflows.get(code=target.resolved_workflow.code)
    desired_release_state = _workflow_edit_release_state(patch)
    mutation = compile_workflow_mutation_plan(
        dag,
        project=target.resolved_project,
        patch=patch,
        release_state=desired_release_state,
    )
    payload = mutation.payload
    merged_spec = mutation.merged_spec
    diff = mutation.diff
    has_changes = mutation.has_changes
    parameter_warnings, parameter_warning_details = (
        workflow_parameter_expression_warnings(merged_spec)
    )
    workflow_state_constraint_details = _workflow_edit_state_constraint_details(
        live_payload,
        has_changes=has_changes,
    )
    workflow_state_constraints = _workflow_edit_constraint_messages(
        workflow_state_constraint_details
    )
    schedule_impact_details = _workflow_edit_schedule_impact_details(
        live_payload,
        desired_release_state=desired_release_state,
    )
    schedule_impacts = _workflow_edit_schedule_impact_messages(schedule_impact_details)
    resolved_data = require_json_object(
        {
            "project": _resolved_project_selection(
                target.resolved_project,
                target.selected_project,
            ),
            "workflow": _resolved_selected_workflow_data(
                code=target.resolved_workflow.code,
                name=merged_spec.workflow.name,
                selection=target.selected_workflow,
            ),
            "patch_file": str(patch_file),
        },
        label="workflow edit resolved",
    )

    if dry_run:
        return _workflow_edit_dry_run_result(
            project_code=target.resolved_project.code,
            workflow_code=target.resolved_workflow.code,
            payload=payload,
            resolved=resolved_data,
            diff=diff,
            workflow_state_constraints=workflow_state_constraints,
            workflow_state_constraint_details=workflow_state_constraint_details,
            schedule_impacts=schedule_impacts,
            schedule_impact_details=schedule_impact_details,
            no_change=not has_changes,
            parameter_warnings=parameter_warnings,
            parameter_warning_details=parameter_warning_details,
        )

    if not has_changes:
        no_change_warning = (
            "patch produced no persistent workflow change; no update request was sent"
        )
        return CommandResult(
            data=require_json_object(
                _serialize_workflow(live_payload),
                label="workflow data",
            ),
            resolved=resolved_data,
            warnings=[
                no_change_warning,
                *schedule_impacts,
                *parameter_warnings,
            ],
            warning_details=[
                require_json_object(
                    WorkflowEditNoChangeWarningDetail(
                        code="workflow_edit_no_persistent_change",
                        message=no_change_warning,
                        no_change=True,
                        request_sent=False,
                    ),
                    label="workflow edit warning detail",
                ),
                *_workflow_edit_schedule_impact_warning_details(
                    schedule_impact_details
                ),
                *_parameter_expression_warning_json_details(parameter_warning_details),
            ],
        )

    if enum_value(live_payload.releaseState) == "ONLINE":
        _raise_workflow_edit_online_error(
            workflow=target.resolved_workflow,
            workflow_state_constraint_details=workflow_state_constraint_details,
        )

    _update_remote_workflow(
        runtime,
        project=target.resolved_project,
        workflow=target.resolved_workflow,
        payload=payload,
    )
    refreshed = runtime.upstream.workflows.get(code=target.resolved_workflow.code)
    return CommandResult(
        data=require_json_object(
            _serialize_workflow(refreshed),
            label="workflow data",
        ),
        resolved=resolved_data,
        warnings=[
            *schedule_impacts,
            *parameter_warnings,
        ],
        warning_details=[
            *_workflow_edit_schedule_impact_warning_details(schedule_impact_details),
            *_parameter_expression_warning_json_details(parameter_warning_details),
        ],
    )


def _run_workflow_result(
    runtime: ServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
    worker_group: str | None,
    tenant: str | None,
    failure_strategy: str | None,
    priority: str | None,
    warning_type: str | None,
    warning_group_id: int | None,
    environment_code: int | None,
    params: list[str],
    dry_run: bool,
    execution_dry_run: bool,
) -> CommandResult:
    target = _resolve_workflow_target(
        runtime,
        workflow=workflow,
        project=project,
    )
    project_preference = load_project_preference_defaults(
        runtime,
        project_code=target.resolved_project.code,
    )
    settings = _workflow_run_settings(
        worker_group=worker_group,
        tenant=tenant,
        failure_strategy=failure_strategy,
        priority=priority,
        warning_type=warning_type,
        warning_group_id=warning_group_id,
        environment_code=environment_code,
        params=params,
        project_preference=project_preference,
        execution_dry_run=execution_dry_run,
    )
    resolved = _workflow_run_resolved(target, settings)
    if dry_run:
        return _workflow_run_dry_run_result(
            project_code=target.resolved_project.code,
            workflow_code=target.resolved_workflow.code,
            settings=settings,
            resolved=resolved,
        )
    try:
        workflow_instance_ids = list(
            runtime.upstream.workflows.run(
                project_code=target.resolved_project.code,
                workflow_code=target.resolved_workflow.code,
                worker_group=settings.worker_group.value,
                tenant_code=settings.tenant.value,
                failure_strategy=settings.failure_strategy.value,
                warning_type=settings.warning_type.value,
                workflow_instance_priority=settings.workflow_instance_priority.value,
                warning_group_id=settings.warning_group_id.value,
                environment_code=settings.environment_code.value,
                start_params=settings.start_params,
                dry_run=settings.execution_dry_run,
            )
        )
    except ApiResultError as error:
        _raise_workflow_run_error(
            error,
            project_code=target.resolved_project.code,
            project_name=target.resolved_project.name,
            workflow=target.resolved_workflow,
        )
    data = require_json_object(
        WorkflowRunData(workflowInstanceIds=workflow_instance_ids),
        label="workflow run data",
    )
    return CommandResult(
        data=data,
        resolved=resolved,
        warnings=_workflow_run_warnings(settings),
        warning_details=_workflow_run_warning_details(settings),
    )


def _run_workflow_task_result(
    runtime: ServiceRuntime,
    *,
    workflow: str | None,
    task: str,
    project: str | None,
    scope: WorkflowRunTaskScope,
    worker_group: str | None,
    tenant: str | None,
    failure_strategy: str | None,
    priority: str | None,
    warning_type: str | None,
    warning_group_id: int | None,
    environment_code: int | None,
    params: list[str],
    dry_run: bool,
    execution_dry_run: bool,
) -> CommandResult:
    target = _resolve_workflow_target(
        runtime,
        workflow=workflow,
        project=project,
    )
    resolved_task = resolve_task(
        task,
        adapter=runtime.upstream.tasks,
        project_code=target.resolved_project.code,
        workflow_code=target.resolved_workflow.code,
    )
    project_preference = load_project_preference_defaults(
        runtime,
        project_code=target.resolved_project.code,
    )
    settings = _workflow_run_settings(
        worker_group=worker_group,
        tenant=tenant,
        failure_strategy=failure_strategy,
        priority=priority,
        warning_type=warning_type,
        warning_group_id=warning_group_id,
        environment_code=environment_code,
        params=params,
        project_preference=project_preference,
        execution_dry_run=execution_dry_run,
    )
    resolved = _workflow_run_resolved(target, settings, task=resolved_task, scope=scope)
    dependency_warning = _workflow_run_task_dependency_warning(scope=scope)
    dependency_warning_detail = _workflow_run_task_dependency_warning_detail(
        scope=scope,
        message=dependency_warning,
    )
    if dry_run:
        return _workflow_run_dry_run_result(
            project_code=target.resolved_project.code,
            workflow_code=target.resolved_workflow.code,
            settings=settings,
            resolved=resolved,
            start_node_list=[resolved_task.code],
            task_scope=scope,
            warnings=[dependency_warning],
            warning_details=[dependency_warning_detail],
        )
    try:
        workflow_instance_ids = list(
            runtime.upstream.workflows.run(
                project_code=target.resolved_project.code,
                workflow_code=target.resolved_workflow.code,
                worker_group=settings.worker_group.value,
                tenant_code=settings.tenant.value,
                start_node_list=[resolved_task.code],
                task_scope=scope,
                failure_strategy=settings.failure_strategy.value,
                warning_type=settings.warning_type.value,
                workflow_instance_priority=settings.workflow_instance_priority.value,
                warning_group_id=settings.warning_group_id.value,
                environment_code=settings.environment_code.value,
                start_params=settings.start_params,
                dry_run=settings.execution_dry_run,
            )
        )
    except ApiResultError as error:
        _raise_workflow_run_error(
            error,
            project_code=target.resolved_project.code,
            project_name=target.resolved_project.name,
            workflow=target.resolved_workflow,
            retry_command="dsctl workflow run-task WORKFLOW --task TASK",
        )
    data = require_json_object(
        WorkflowRunData(workflowInstanceIds=workflow_instance_ids),
        label="workflow run-task data",
    )
    return CommandResult(
        data=data,
        resolved=resolved,
        warnings=[
            *_workflow_run_warnings(settings),
            dependency_warning,
        ],
        warning_details=[
            *_workflow_run_warning_details(settings),
            dependency_warning_detail,
        ],
    )


def _backfill_workflow_result(
    runtime: ServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
    start: str | None,
    end: str | None,
    dates: list[str],
    task: str | None,
    scope: WorkflowRunTaskScope,
    run_mode: str | None,
    expected_parallelism_number: int,
    complement_dependent_mode: str | None,
    all_level_dependent: bool,
    execution_order: str | None,
    worker_group: str | None,
    tenant: str | None,
    failure_strategy: str | None,
    priority: str | None,
    warning_type: str | None,
    warning_group_id: int | None,
    environment_code: int | None,
    params: list[str],
    dry_run: bool,
    execution_dry_run: bool,
) -> CommandResult:
    target = _resolve_workflow_target(
        runtime,
        workflow=workflow,
        project=project,
    )
    resolved_task = (
        None
        if task is None
        else resolve_task(
            task,
            adapter=runtime.upstream.tasks,
            project_code=target.resolved_project.code,
            workflow_code=target.resolved_workflow.code,
        )
    )
    project_preference = load_project_preference_defaults(
        runtime,
        project_code=target.resolved_project.code,
    )
    settings = _workflow_run_settings(
        worker_group=worker_group,
        tenant=tenant,
        failure_strategy=failure_strategy,
        priority=priority,
        warning_type=warning_type,
        warning_group_id=warning_group_id,
        environment_code=environment_code,
        params=params,
        project_preference=project_preference,
        execution_dry_run=execution_dry_run,
    )
    backfill_settings = _workflow_backfill_settings(
        start=start,
        end=end,
        dates=dates,
        run_mode=run_mode,
        expected_parallelism_number=expected_parallelism_number,
        complement_dependent_mode=complement_dependent_mode,
        all_level_dependent=all_level_dependent,
        execution_order=execution_order,
    )
    start_node_list = None if resolved_task is None else [resolved_task.code]
    task_scope = None if resolved_task is None else scope
    resolved = _workflow_run_resolved(
        target,
        settings,
        task=resolved_task,
        scope=task_scope,
        backfill_settings=backfill_settings,
    )
    dependency_warning = (
        None
        if resolved_task is None
        else _workflow_run_task_dependency_warning(scope=scope)
    )
    dependency_warning_detail = (
        None
        if dependency_warning is None
        else _workflow_run_task_dependency_warning_detail(
            scope=scope,
            message=dependency_warning,
        )
    )
    extra_warnings = [] if dependency_warning is None else [dependency_warning]
    extra_warning_details = (
        [] if dependency_warning_detail is None else [dependency_warning_detail]
    )
    if dry_run:
        return _workflow_backfill_dry_run_result(
            project_code=target.resolved_project.code,
            workflow_code=target.resolved_workflow.code,
            settings=settings,
            backfill_settings=backfill_settings,
            resolved=resolved,
            start_node_list=start_node_list,
            task_scope=task_scope,
            warnings=extra_warnings,
            warning_details=extra_warning_details,
        )
    try:
        workflow_instance_ids = list(
            runtime.upstream.workflows.backfill(
                project_code=target.resolved_project.code,
                workflow_code=target.resolved_workflow.code,
                schedule_time=backfill_settings.schedule_time,
                run_mode=backfill_settings.run_mode.value,
                expected_parallelism_number=(
                    backfill_settings.expected_parallelism_number
                ),
                complement_dependent_mode=(
                    backfill_settings.complement_dependent_mode.value
                ),
                all_level_dependent=backfill_settings.all_level_dependent,
                execution_order=backfill_settings.execution_order.value,
                worker_group=settings.worker_group.value,
                tenant_code=settings.tenant.value,
                start_node_list=start_node_list,
                task_scope=task_scope,
                failure_strategy=settings.failure_strategy.value,
                warning_type=settings.warning_type.value,
                workflow_instance_priority=settings.workflow_instance_priority.value,
                warning_group_id=settings.warning_group_id.value,
                environment_code=settings.environment_code.value,
                start_params=settings.start_params,
                dry_run=settings.execution_dry_run,
            )
        )
    except ApiResultError as error:
        _raise_workflow_run_error(
            error,
            project_code=target.resolved_project.code,
            project_name=target.resolved_project.name,
            workflow=target.resolved_workflow,
            retry_command="dsctl workflow backfill WORKFLOW --start START --end END",
        )
    data = require_json_object(
        WorkflowRunData(workflowInstanceIds=workflow_instance_ids),
        label="workflow backfill data",
    )
    return CommandResult(
        data=data,
        resolved=resolved,
        warnings=[
            *_workflow_run_warnings(settings),
            *extra_warnings,
        ],
        warning_details=[
            *_workflow_run_warning_details(settings),
            *extra_warning_details,
        ],
    )


def _workflow_run_settings(
    *,
    worker_group: str | None,
    tenant: str | None,
    failure_strategy: str | None,
    priority: str | None,
    warning_type: str | None,
    warning_group_id: int | None,
    environment_code: int | None,
    params: list[str],
    project_preference: ProjectPreferenceDefaults | None,
    execution_dry_run: bool,
) -> _WorkflowRunSettings:
    start_params, start_param_names = _workflow_run_start_params(params)
    return _WorkflowRunSettings(
        worker_group=select_worker_group(
            worker_group,
            project_preference=project_preference,
        ),
        tenant=_select_workflow_run_tenant_code(
            tenant,
            project_preference=project_preference,
        ),
        failure_strategy=_select_workflow_run_enum(
            failure_strategy,
            project_preference_value=None,
            default="CONTINUE",
            choices=_WORKFLOW_RUN_FAILURE_STRATEGIES,
            label="failure strategy",
            option_name="failure-strategy",
            preference_field=None,
        ),
        warning_type=_select_workflow_run_enum(
            warning_type,
            project_preference_value=(
                None if project_preference is None else project_preference.warning_type
            ),
            default="NONE",
            choices=_WORKFLOW_RUN_WARNING_TYPES,
            label="warning type",
            option_name="warning-type",
            preference_field="warningType",
        ),
        workflow_instance_priority=_select_workflow_run_enum(
            priority,
            project_preference_value=(
                None if project_preference is None else project_preference.task_priority
            ),
            default="MEDIUM",
            choices=_WORKFLOW_RUN_PRIORITIES,
            label="workflow instance priority",
            option_name="priority",
            preference_field="taskPriority",
        ),
        warning_group_id=_select_workflow_run_optional_int(
            warning_group_id,
            project_preference_value=(
                None
                if project_preference is None
                else project_preference.warning_group_id
            ),
            label="warning-group-id",
        ),
        environment_code=_select_workflow_run_optional_int(
            environment_code,
            project_preference_value=(
                None
                if project_preference is None
                else project_preference.environment_code
            ),
            label="environment-code",
        ),
        start_params=start_params,
        start_param_names=start_param_names,
        execution_dry_run=execution_dry_run,
    )


def _select_workflow_run_tenant_code(
    explicit_tenant_code: str | None,
    *,
    project_preference: ProjectPreferenceDefaults | None,
) -> SelectedValue:
    normalized_flag = optional_text(explicit_tenant_code)
    if normalized_flag is not None:
        return SelectedValue(value=normalized_flag, source="flag")

    if project_preference is not None and project_preference.tenant_code is not None:
        return SelectedValue(
            value=project_preference.tenant_code,
            source="project_preference",
        )

    return SelectedValue(value="default", source="default")


def _select_workflow_run_enum(
    explicit_value: str | None,
    *,
    project_preference_value: str | None,
    default: str,
    choices: tuple[str, ...],
    label: str,
    option_name: str,
    preference_field: str | None,
) -> SelectedValue:
    normalized_flag = optional_text(explicit_value)
    if normalized_flag is not None:
        return SelectedValue(
            value=_normalized_workflow_run_enum(
                normalized_flag,
                choices=choices,
                label=label,
                suggestion=f"Pass --{option_name} as one of: {', '.join(choices)}.",
            ),
            source="flag",
        )

    if project_preference_value is not None:
        return SelectedValue(
            value=_normalized_workflow_run_enum(
                project_preference_value,
                choices=choices,
                label=label,
                suggestion=(
                    "Fix the remote value with `dsctl project-preference update` "
                    "before retrying."
                ),
                preference_field=preference_field,
            ),
            source="project_preference",
        )

    return SelectedValue(value=default, source="default")


def _normalized_workflow_run_enum(
    value: str,
    *,
    choices: tuple[str, ...],
    label: str,
    suggestion: str,
    preference_field: str | None = None,
    aliases: dict[str, str] | None = None,
) -> str:
    normalized = value.strip().upper().replace("-", "_")
    if aliases is not None:
        normalized = aliases.get(normalized, normalized)
    if normalized in choices:
        return normalized
    message = f"Workflow run {label} must be one of: {', '.join(choices)}"
    if preference_field is not None:
        raise ConflictError(
            message,
            details={"field": preference_field, "value": value},
            suggestion=suggestion,
        )
    raise UserInputError(
        message,
        details={label.replace(" ", "_"): value},
        suggestion=suggestion,
    )


def _select_workflow_run_optional_int(
    explicit_value: int | None,
    *,
    project_preference_value: int | None,
    label: str,
) -> _SelectedOptionalInt:
    if explicit_value is not None:
        return _SelectedOptionalInt(
            value=require_non_negative_int(explicit_value, label=label),
            source="flag",
        )
    if project_preference_value is not None:
        return _SelectedOptionalInt(
            value=project_preference_value,
            source="project_preference",
        )
    return _SelectedOptionalInt(value=None, source="default")


def _workflow_backfill_settings(
    *,
    start: str | None,
    end: str | None,
    dates: list[str],
    run_mode: str | None,
    expected_parallelism_number: int,
    complement_dependent_mode: str | None,
    all_level_dependent: bool,
    execution_order: str | None,
) -> _WorkflowBackfillSettings:
    return _WorkflowBackfillSettings(
        schedule_time=_workflow_backfill_schedule_time(
            start=start,
            end=end,
            dates=dates,
        ),
        schedule_time_mode=_workflow_backfill_time_mode(
            start=start,
            end=end,
            dates=dates,
        ),
        run_mode=_select_workflow_backfill_enum(
            run_mode,
            default="RUN_MODE_SERIAL",
            choices=_WORKFLOW_BACKFILL_RUN_MODES,
            aliases={
                "SERIAL": "RUN_MODE_SERIAL",
                "PARALLEL": "RUN_MODE_PARALLEL",
            },
            label="run mode",
            option_name="run-mode",
        ),
        expected_parallelism_number=require_non_negative_int(
            expected_parallelism_number,
            label="expected-parallelism-number",
        ),
        complement_dependent_mode=_select_workflow_backfill_enum(
            complement_dependent_mode,
            default="OFF_MODE",
            choices=_WORKFLOW_BACKFILL_COMPLEMENT_DEPENDENT_MODES,
            aliases={
                "OFF": "OFF_MODE",
                "ALL": "ALL_DEPENDENT",
            },
            label="complement dependent mode",
            option_name="complement-dependent-mode",
        ),
        all_level_dependent=all_level_dependent,
        execution_order=_select_workflow_backfill_enum(
            execution_order,
            default="DESC_ORDER",
            choices=_WORKFLOW_BACKFILL_EXECUTION_ORDERS,
            aliases={
                "DESC": "DESC_ORDER",
                "ASC": "ASC_ORDER",
            },
            label="execution order",
            option_name="execution-order",
        ),
    )


def _select_workflow_backfill_enum(
    explicit_value: str | None,
    *,
    default: str,
    choices: tuple[str, ...],
    aliases: dict[str, str],
    label: str,
    option_name: str,
) -> SelectedValue:
    normalized_flag = optional_text(explicit_value)
    if normalized_flag is not None:
        return SelectedValue(
            value=_normalized_workflow_run_enum(
                normalized_flag,
                choices=choices,
                aliases=aliases,
                label=label,
                suggestion=f"Pass --{option_name} as one of: {', '.join(aliases)}.",
            ),
            source="flag",
        )
    return SelectedValue(value=default, source="default")


def _workflow_backfill_schedule_time(
    *,
    start: str | None,
    end: str | None,
    dates: list[str],
) -> str:
    normalized_dates = _workflow_backfill_dates(dates)
    normalized_start = optional_text(start)
    normalized_end = optional_text(end)
    if normalized_dates:
        if normalized_start is not None or normalized_end is not None:
            message = "Workflow backfill accepts either --date or --start/--end"
            raise UserInputError(
                message,
                suggestion=(
                    "Use repeated --date values, or pass both --start and --end."
                ),
            )
        payload = {"complementScheduleDateList": ",".join(normalized_dates)}
    else:
        if normalized_start is None or normalized_end is None:
            message = "Workflow backfill requires --start and --end, or --date"
            raise UserInputError(
                message,
                suggestion=(
                    "Pass both --start 'yyyy-MM-dd HH:mm:ss' and --end "
                    "'yyyy-MM-dd HH:mm:ss', or repeat --date for explicit "
                    "complement schedule dates."
                ),
            )
        payload = {
            "complementStartDate": normalized_start,
            "complementEndDate": normalized_end,
        }
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def _workflow_backfill_time_mode(
    *,
    start: str | None,
    end: str | None,
    dates: list[str],
) -> WorkflowBackfillTimeMode:
    del start, end
    return "dates" if _workflow_backfill_dates(dates) else "range"


def _workflow_backfill_dates(dates: list[str]) -> list[str]:
    return [
        require_non_empty_text(date, label="date")
        for date in dates
        if optional_text(date) is not None
    ]


def _workflow_run_start_params(params: list[str]) -> tuple[str | None, list[str]]:
    if not params:
        return None, []
    payload: dict[str, str] = {}
    for item in params:
        key, separator, raw_value = item.partition("=")
        normalized_key = key.strip()
        if not separator or not normalized_key:
            message = f"Invalid --param value {item!r}; expected KEY=VALUE"
            raise UserInputError(
                message,
                suggestion=(
                    "Pass runtime parameters as `--param name=value`; repeat the "
                    "option for multiple workflow start parameters."
                ),
            )
        if normalized_key in payload:
            message = (
                f"Workflow start parameter {normalized_key!r} was specified more "
                "than once"
            )
            raise UserInputError(
                message,
                suggestion="Pass each workflow start parameter name only once.",
            )
        payload[normalized_key] = raw_value
    return (
        json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
        list(payload),
    )


def _workflow_run_resolved(
    target: _ResolvedWorkflowTarget,
    settings: _WorkflowRunSettings,
    *,
    task: ResolvedTask | None = None,
    scope: WorkflowRunTaskScope | None = None,
    backfill_settings: _WorkflowBackfillSettings | None = None,
) -> JsonObject:
    resolved: JsonObject = {
        "project": _resolved_project_selection(
            target.resolved_project,
            target.selected_project,
        ),
        "workflow": _resolved_workflow_selection(
            target.resolved_workflow,
            target.selected_workflow,
        ),
        "worker_group": selected_value_data(settings.worker_group),
        "tenant": selected_value_data(settings.tenant),
        "failure_strategy": selected_value_data(settings.failure_strategy),
        "warning_type": selected_value_data(settings.warning_type),
        "workflow_instance_priority": selected_value_data(
            settings.workflow_instance_priority
        ),
        "warning_group_id": _selected_optional_int_data(settings.warning_group_id),
        "environment_code": _selected_optional_int_data(settings.environment_code),
        "start_params": {
            "names": settings.start_param_names,
            "count": len(settings.start_param_names),
            "source": "flag" if settings.start_param_names else "default",
        },
        "execution_dry_run": settings.execution_dry_run,
    }
    if task is not None:
        resolved["task"] = _resolved_task_data(task)
    if scope is not None:
        resolved["scope"] = scope
    if backfill_settings is not None:
        resolved["backfill"] = {
            "schedule_time_mode": backfill_settings.schedule_time_mode,
            "run_mode": selected_value_data(backfill_settings.run_mode),
            "expected_parallelism_number": (
                backfill_settings.expected_parallelism_number
            ),
            "complement_dependent_mode": selected_value_data(
                backfill_settings.complement_dependent_mode
            ),
            "all_level_dependent": backfill_settings.all_level_dependent,
            "execution_order": selected_value_data(backfill_settings.execution_order),
        }
    return require_json_object(resolved, label="workflow run resolved")


def _selected_optional_int_data(selected: _SelectedOptionalInt) -> JsonObject:
    return require_json_object(
        {
            "value": selected.value,
            "source": selected.source,
        },
        label="selected optional integer",
    )


def _workflow_run_dry_run_result(
    *,
    project_code: int,
    workflow_code: int,
    settings: _WorkflowRunSettings,
    resolved: JsonObject,
    start_node_list: list[int] | None = None,
    task_scope: WorkflowRunTaskScope | None = None,
    warnings: list[str] | None = None,
    warning_details: list[JsonObject] | None = None,
) -> CommandResult:
    return dry_run_result(
        method="POST",
        path=f"/projects/{project_code}/executors/start-workflow-instance",
        form_data=_workflow_run_form_data(
            workflow_code=workflow_code,
            settings=settings,
            start_node_list=start_node_list,
            task_scope=task_scope,
        ),
        resolved=resolved,
        warnings=[
            *_workflow_run_warnings(settings),
            *(warnings or []),
        ],
        warning_details=[
            *_workflow_run_warning_details(settings),
            *(warning_details or []),
        ],
    )


def _workflow_backfill_dry_run_result(
    *,
    project_code: int,
    workflow_code: int,
    settings: _WorkflowRunSettings,
    backfill_settings: _WorkflowBackfillSettings,
    resolved: JsonObject,
    start_node_list: list[int] | None = None,
    task_scope: WorkflowRunTaskScope | None = None,
    warnings: list[str] | None = None,
    warning_details: list[JsonObject] | None = None,
) -> CommandResult:
    return dry_run_result(
        method="POST",
        path=f"/projects/{project_code}/executors/start-workflow-instance",
        form_data=_workflow_run_form_data(
            workflow_code=workflow_code,
            settings=settings,
            start_node_list=start_node_list,
            task_scope=task_scope,
            backfill_settings=backfill_settings,
        ),
        resolved=resolved,
        warnings=[
            *_workflow_run_warnings(settings),
            *(warnings or []),
        ],
        warning_details=[
            *_workflow_run_warning_details(settings),
            *(warning_details or []),
        ],
    )


def _workflow_run_form_data(
    *,
    workflow_code: int,
    settings: _WorkflowRunSettings,
    start_node_list: list[int] | None,
    task_scope: WorkflowRunTaskScope | None,
    backfill_settings: _WorkflowBackfillSettings | None = None,
) -> JsonObject:
    form: JsonObject = {
        "workflowDefinitionCode": workflow_code,
        "scheduleTime": (
            _workflow_run_start_process_schedule_time()
            if backfill_settings is None
            else backfill_settings.schedule_time
        ),
        "failureStrategy": settings.failure_strategy.value,
        "execType": "START_PROCESS" if backfill_settings is None else "COMPLEMENT_DATA",
        "taskDependType": _workflow_run_task_depend_type(task_scope),
        "warningType": settings.warning_type.value,
        "workflowInstancePriority": settings.workflow_instance_priority.value,
        "workerGroup": settings.worker_group.value,
        "tenantCode": settings.tenant.value,
        "dryRun": 1 if settings.execution_dry_run else 0,
    }
    if backfill_settings is not None:
        form.update(
            {
                "runMode": backfill_settings.run_mode.value,
                "expectedParallelismNumber": (
                    backfill_settings.expected_parallelism_number
                ),
                "complementDependentMode": (
                    backfill_settings.complement_dependent_mode.value
                ),
                "allLevelDependent": backfill_settings.all_level_dependent,
                "executionOrder": backfill_settings.execution_order.value,
            }
        )
    start_nodes = _workflow_run_start_node_list(start_node_list)
    if start_nodes is not None:
        form["startNodeList"] = start_nodes
    if settings.warning_group_id.value is not None:
        form["warningGroupId"] = settings.warning_group_id.value
    if settings.environment_code.value is not None:
        form["environmentCode"] = settings.environment_code.value
    if settings.start_params is not None:
        form["startParams"] = settings.start_params
    return require_json_object(form, label="workflow run form data")


def _workflow_run_start_process_schedule_time() -> str:
    timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
    return json.dumps(
        {
            "complementStartDate": timestamp,
            "complementEndDate": timestamp,
        },
        ensure_ascii=True,
        separators=(",", ":"),
    )


def _workflow_run_task_depend_type(scope: WorkflowRunTaskScope | None) -> str:
    if scope == "self":
        return "TASK_ONLY"
    if scope == "pre":
        return "TASK_PRE"
    return "TASK_POST"


def _workflow_run_start_node_list(start_node_list: list[int] | None) -> str | None:
    if not start_node_list:
        return None
    return ",".join(str(code) for code in start_node_list)


def _workflow_run_warnings(settings: _WorkflowRunSettings) -> list[str]:
    if not settings.execution_dry_run:
        return []
    return [_workflow_execution_dry_run_warning()]


def _workflow_run_warning_details(
    settings: _WorkflowRunSettings,
) -> list[JsonObject]:
    if not settings.execution_dry_run:
        return []
    return [
        require_json_object(
            WorkflowExecutionDryRunWarningDetail(
                code="workflow_execution_dry_run",
                message=_workflow_execution_dry_run_warning(),
                blocking=False,
                request_sent=True,
            ),
            label="workflow execution dry-run warning detail",
        )
    ]


def _workflow_execution_dry_run_warning() -> str:
    return (
        "DS execution dry-run is enabled; DolphinScheduler will create dry-run "
        "workflow/task instances and skip task plugin trigger execution."
    )


def _workflow_run_task_dependency_warning_detail(
    *,
    scope: WorkflowRunTaskScope,
    message: str,
) -> JsonObject:
    return require_json_object(
        WorkflowRunTaskDependencyWarningDetail(
            code="workflow_run_task_dependent_context",
            message=message,
            blocking=False,
            scope=scope,
            dependent_resolution=(
                "DS DEPENDENT tasks resolve dependency status from workflow/task "
                "instances in the dependency date interval; running only a "
                "selected task may leave referenced workflow or task instances "
                "absent or unsuccessful."
            ),
        ),
        label="workflow run-task warning detail",
    )


def _delete_workflow_result(
    runtime: ServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
) -> CommandResult:
    target = _resolve_workflow_target(
        runtime,
        workflow=workflow,
        project=project,
    )
    adapter = runtime.upstream.workflows
    try:
        current = adapter.get(code=target.resolved_workflow.code)
        adapter.delete(
            project_code=target.resolved_project.code,
            workflow_code=target.resolved_workflow.code,
        )
    except ApiResultError as error:
        _raise_workflow_delete_error(
            error,
            project=target.resolved_project,
            workflow=target.resolved_workflow,
        )
    return CommandResult(
        data=require_json_object(
            DeleteWorkflowData(
                deleted=True,
                workflow=_serialize_workflow(current),
            ),
            label="workflow delete data",
        ),
        resolved={
            "project": _resolved_project_selection(
                target.resolved_project,
                target.selected_project,
            ),
            "workflow": _resolved_workflow_selection(
                target.resolved_workflow,
                target.selected_workflow,
            ),
        },
    )


def _set_workflow_release_state_result(
    runtime: ServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
    action: Literal["online", "offline"],
) -> CommandResult:
    target = _resolve_workflow_target(
        runtime,
        workflow=workflow,
        project=project,
    )
    payload = runtime.upstream.workflows.get(code=target.resolved_workflow.code)
    try:
        if action == "online":
            runtime.upstream.workflows.online(
                project_code=target.resolved_project.code,
                workflow_code=target.resolved_workflow.code,
            )
        else:
            runtime.upstream.workflows.offline(
                project_code=target.resolved_project.code,
                workflow_code=target.resolved_workflow.code,
            )
    except ApiResultError as exc:
        _raise_workflow_release_error(
            exc,
            workflow=target.resolved_workflow,
            action=action,
        )
    refreshed = runtime.upstream.workflows.get(code=target.resolved_workflow.code)
    return CommandResult(
        data=require_json_object(
            _serialize_workflow(refreshed),
            label="workflow data",
        ),
        resolved={
            "project": _resolved_project_selection(
                target.resolved_project,
                target.selected_project,
            ),
            "workflow": _resolved_workflow_selection(
                target.resolved_workflow,
                target.selected_workflow,
            ),
        },
        warnings=_workflow_release_warnings(payload, action=action),
        warning_details=_workflow_release_warning_details(payload, action=action),
    )


def _load_workflow_spec_or_error(path: Path) -> WorkflowSpec:
    try:
        return load_workflow_spec(path)
    except ValueError as exc:
        raise UserInputError(
            str(exc),
            details={"file": str(path)},
            suggestion=(
                "Run `dsctl template workflow` to inspect the stable YAML surface, "
                "then run `dsctl lint workflow PATH` before retrying create."
            ),
        ) from exc


def _resolve_create_project(
    runtime: ServiceRuntime,
    *,
    project: str | None,
    spec: WorkflowSpec,
) -> tuple[ResolvedProject, dict[str, int | str | None]]:
    if project is not None:
        selected_project = require_project_selection(project, runtime=runtime)
        resolved_project = resolve_project(
            selected_project.value,
            adapter=runtime.upstream.projects,
        )
        return resolved_project, _resolved_project_selection(
            resolved_project,
            selected_project,
        )
    if spec.workflow.project is not None:
        resolved_project = resolve_project(
            spec.workflow.project,
            adapter=runtime.upstream.projects,
        )
        return resolved_project, {
            **cast("dict[str, int | str | None]", resolved_project.to_data()),
            "source": "file",
        }
    selected_project = require_project_selection(None, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    return resolved_project, _resolved_project_selection(
        resolved_project,
        selected_project,
    )


def _resolved_file_workflow_data(
    workflow: ResolvedWorkflow,
) -> dict[str, int | str | None]:
    return {
        **cast("dict[str, int | str | None]", workflow.to_data()),
        "source": "file",
    }


def _prepare_workflow_schedule_create(
    runtime: ServiceRuntime,
    *,
    project_code: int,
    spec: WorkflowSpec,
    confirm_risk: str | None,
) -> tuple[ScheduleCreateInput | None, SchedulePreviewData | None, list[str]]:
    schedule_input = _workflow_schedule_input(spec)
    if schedule_input is None:
        return None, None, []
    require_schedule_block_create_compatible(spec)
    preview = preview_schedule(
        runtime,
        project_code=project_code,
        schedule_input=schedule_input,
    )
    confirmation_payload = _workflow_schedule_create_confirmation_payload(
        project_code=project_code,
        workflow_name=spec.workflow.name,
        schedule_input=schedule_input,
    )
    require_high_frequency_confirmation(
        action="workflow.create",
        confirmation=confirm_risk,
        preview=preview,
        schedule_payload=confirmation_payload,
    )
    return (
        schedule_input,
        preview,
        confirmed_preview_warnings(preview),
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
        environment_code=0,
    )


def _workflow_create_dry_run_requests(
    *,
    project_code: int,
    workflow_name: str,
    workflow_payload: WorkflowCreatePayload,
    workflow_should_online: bool,
    schedule_input: ScheduleCreateInput | None,
    schedule_should_online: bool,
) -> list[JsonObject]:
    workflow_form = require_json_object(
        workflow_payload,
        label="workflow create form data",
    )
    requests: list[JsonObject] = [
        build_dry_run_request(
            method="POST",
            path=f"/projects/{project_code}/workflow-definition",
            form_data=workflow_form,
        )
    ]
    created_workflow_code = f"<{workflow_name}:created_workflow_code>"
    if workflow_should_online:
        requests.append(
            build_dry_run_request(
                method="POST",
                path=(
                    f"/projects/{project_code}/workflow-definition/"
                    f"{created_workflow_code}/release"
                ),
                form_data={"releaseState": "ONLINE"},
            )
        )
    if schedule_input is None:
        return requests
    created_schedule_id = f"<{workflow_name}:created_schedule_id>"
    requests.append(
        build_dry_run_request(
            method="POST",
            path="/v2/schedules",
            json_body={
                "workflowDefinitionCode": created_workflow_code,
                "crontab": schedule_input["crontab"],
                "startTime": schedule_input["start_time"],
                "endTime": schedule_input["end_time"],
                "timezoneId": schedule_input["timezone_id"],
                "failureStrategy": schedule_input["failure_strategy"],
                "warningType": schedule_input["warning_type"],
                "warningGroupId": schedule_input["warning_group_id"],
                "workflowInstancePriority": schedule_input[
                    "workflow_instance_priority"
                ],
                "workerGroup": schedule_input["worker_group"],
                "tenantCode": schedule_input["tenant_code"],
                "environmentCode": schedule_input["environment_code"],
            },
        )
    )
    if schedule_should_online:
        requests.append(
            build_dry_run_request(
                method="POST",
                path=f"/projects/{project_code}/schedules/{created_schedule_id}/online",
            )
        )
    return requests


def _workflow_create_dry_run_result(
    *,
    file: Path,
    spec: WorkflowSpec,
    resolved_project_data: dict[str, int | str | None],
    project_code: int,
    workflow_payload: WorkflowCreatePayload,
    schedule_input: ScheduleCreateInput | None,
    schedule_preview: SchedulePreviewData | None,
    parameter_warnings: list[str],
    parameter_warning_details: list[ParameterExpressionWarningDetail],
) -> CommandResult:
    return dry_run_result(
        method="POST",
        path=f"/projects/{project_code}/workflow-definition",
        form_data=require_json_object(
            workflow_payload,
            label="workflow create form data",
        ),
        requests=_workflow_create_dry_run_requests(
            project_code=project_code,
            workflow_name=spec.workflow.name,
            workflow_payload=workflow_payload,
            schedule_input=schedule_input,
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
        },
        extra_data=_workflow_create_dry_run_schedule_data(
            project_code=project_code,
            workflow_name=spec.workflow.name,
            schedule_input=schedule_input,
            schedule_preview=schedule_preview,
        ),
        warnings=parameter_warnings,
        warning_details=_parameter_expression_warning_json_details(
            parameter_warning_details
        ),
    )


def _workflow_schedule_create_confirmation_payload(
    *,
    project_code: int,
    workflow_name: str,
    schedule_input: ScheduleCreateInput,
) -> dict[str, object]:
    return {
        "project_code": project_code,
        "workflow_name": workflow_name,
        "schedule": schedule_input,
    }


def _workflow_create_dry_run_schedule_data(
    *,
    project_code: int,
    workflow_name: str,
    schedule_input: ScheduleCreateInput | None,
    schedule_preview: SchedulePreviewData | None,
) -> JsonObject | None:
    if schedule_input is None or schedule_preview is None:
        return None
    return require_json_object(
        WorkflowCreateScheduleDryRunData(
            schedule_preview=schedule_preview,
            schedule_confirmation=schedule_confirmation_data(
                action="workflow.create",
                preview=schedule_preview,
                schedule_payload=_workflow_schedule_create_confirmation_payload(
                    project_code=project_code,
                    workflow_name=workflow_name,
                    schedule_input=schedule_input,
                ),
            ),
        ),
        label="workflow create dry-run schedule data",
    )


def _create_remote_workflow(
    runtime: ServiceRuntime,
    *,
    project_code: int,
    payload: WorkflowCreatePayload,
    project: ResolvedProject,
    workflow_name: str,
) -> None:
    try:
        runtime.upstream.workflows.create(
            project_code=project_code,
            name=payload["name"],
            description=payload["description"],
            global_params=payload["globalParams"],
            locations=payload["locations"],
            timeout=payload["timeout"],
            task_relation_json=payload["taskRelationJson"],
            task_definition_json=payload["taskDefinitionJson"],
            execution_type=payload["executionType"],
        )
    except ApiResultError as exc:
        _raise_workflow_create_error(
            exc,
            project=project,
            workflow_name=workflow_name,
        )


def _update_remote_workflow(
    runtime: ServiceRuntime,
    *,
    project: ResolvedProject,
    workflow: ResolvedWorkflow,
    payload: WorkflowUpdatePayload,
) -> None:
    try:
        runtime.upstream.workflows.update(
            project_code=project.code,
            workflow_code=workflow.code,
            name=payload["name"],
            description=payload["description"],
            global_params=payload["globalParams"],
            locations=payload["locations"],
            timeout=payload["timeout"],
            task_relation_json=payload["taskRelationJson"],
            task_definition_json=payload["taskDefinitionJson"],
            execution_type=payload["executionType"],
            release_state=payload["releaseState"],
        )
    except ApiResultError as exc:
        _raise_workflow_update_error(
            exc,
            project=project,
            workflow=workflow,
            workflow_name=payload["name"],
        )


def _apply_workflow_release_and_schedule(
    runtime: ServiceRuntime,
    *,
    project_code: int,
    resolved_workflow: ResolvedWorkflow,
    spec: WorkflowSpec,
    schedule_input: ScheduleCreateInput | None,
) -> None:
    if spec.workflow.release_state.value == "ONLINE":
        try:
            runtime.upstream.workflows.online(
                project_code=project_code,
                workflow_code=resolved_workflow.code,
            )
        except ApiResultError as exc:
            _raise_workflow_release_error(
                exc,
                workflow=resolved_workflow,
                action="online",
            )
    if schedule_input is None:
        return
    created_schedule_id = _create_workflow_schedule(
        runtime,
        workflow_code=resolved_workflow.code,
        workflow_name=resolved_workflow.name,
        schedule_input=schedule_input,
    )
    if spec.schedule is None or spec.schedule.desired_release_state().value != "ONLINE":
        return
    try:
        runtime.upstream.schedules.online(schedule_id=created_schedule_id)
    except ApiResultError as exc:
        raise translate_schedule_api_error(
            exc,
            operation="online",
            schedule_id=created_schedule_id,
            workflow_code=resolved_workflow.code,
            workflow_name=resolved_workflow.name,
        ) from exc


def _create_workflow_schedule(
    runtime: ServiceRuntime,
    *,
    workflow_code: int,
    workflow_name: str | None,
    schedule_input: ScheduleCreateInput,
) -> int:
    try:
        created_schedule = runtime.upstream.schedules.create(
            workflow_code=workflow_code,
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
    except ApiResultError as exc:
        raise translate_schedule_api_error(
            exc,
            operation="create",
            workflow_code=workflow_code,
            workflow_name=workflow_name,
        ) from exc
    return require_resource_int(
        created_schedule.id,
        resource=SCHEDULE_RESOURCE,
        field_name="schedule.id",
    )


def _resolved_project_selection(
    project: ResolvedProject,
    selection: SelectedValue,
) -> dict[str, int | str | None]:
    return with_selection_source(cast("SelectionData", project.to_data()), selection)


def _resolve_workflow_target(
    runtime: ServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
) -> _ResolvedWorkflowTarget:
    selected_project = require_project_selection(project, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    selected_workflow = require_workflow_selection(workflow, runtime=runtime)
    resolved_workflow = resolve_workflow(
        selected_workflow.value,
        adapter=runtime.upstream.workflows,
        project_code=resolved_project.code,
    )
    return _ResolvedWorkflowTarget(
        selected_project=selected_project,
        resolved_project=resolved_project,
        selected_workflow=selected_workflow,
        resolved_workflow=resolved_workflow,
    )


def _resolved_workflow_selection(
    workflow: ResolvedWorkflow,
    selection: SelectedValue,
) -> dict[str, int | str | None]:
    return with_selection_source(cast("SelectionData", workflow.to_data()), selection)


def _resolved_task_data(task: ResolvedTask) -> dict[str, int | str | None]:
    return cast("dict[str, int | str | None]", task.to_data())


def _resolved_selected_workflow_data(
    *,
    code: int,
    name: str,
    selection: SelectedValue,
) -> dict[str, int | str | None]:
    return with_selection_source({"code": code, "name": name}, selection)


def _normalized_output_format(value: str) -> WorkflowOutputFormat:
    normalized = value.strip().lower()
    if normalized == "json":
        return "json"
    if normalized == "yaml":
        return "yaml"
    message = "Workflow output format must be 'json' or 'yaml'"
    raise UserInputError(
        message,
        details={"format": value},
        suggestion="Pass `--format json` or `--format yaml`.",
    )


def _normalized_task_run_scope(value: str) -> WorkflowRunTaskScope:
    normalized = value.strip().lower()
    if normalized in {"self", "pre", "post"}:
        return cast("WorkflowRunTaskScope", normalized)
    message = "Task execution scope must be one of: self, pre, post"
    raise UserInputError(
        message,
        details={"scope": value},
        suggestion="Pass `--scope self`, `--scope pre`, or `--scope post`.",
    )


def _workflow_run_task_dependency_warning(*, scope: WorkflowRunTaskScope) -> str:
    scope_note = {
        "self": "only the selected task",
        "pre": "the selected task and upstream tasks",
        "post": "the selected task and downstream tasks",
    }[scope]
    return (
        "Dependent downstream nodes may fail if their referenced task, whole "
        "workflow, or scheduled dependency instance has not produced a "
        f"successful run; this request starts {scope_note}."
    )


def _raise_workflow_release_error(
    error: ApiResultError,
    *,
    workflow: ResolvedWorkflow,
    action: Literal["online", "offline"],
) -> None:
    if error.result_code == WORKFLOW_DEFINITION_NOT_EXIST:
        message = f"Workflow '{workflow.name}' does not exist."
        raise NotFoundError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "code": workflow.code,
                "name": workflow.name,
            },
        ) from error
    if action == "online" and _is_subworkflow_not_online_release_error(error):
        message = (
            "This workflow cannot be brought online until all referenced "
            "sub-workflows are already online."
        )
        raise InvalidStateError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "code": workflow.code,
                "name": workflow.name,
                "action": action,
            },
            suggestion=(
                "Run `dsctl workflow lineage dependent-tasks WORKFLOW --project "
                "PROJECT` to inspect sub-workflow references, bring those "
                "sub-workflows online, then retry `dsctl workflow online`."
            ),
        ) from error
    raise error


def _is_subworkflow_not_online_release_error(error: ApiResultError) -> bool:
    """Return whether one release error means a referenced sub-workflow is offline."""
    if error.result_code == WORKFLOW_DEFINITION_NOT_RELEASE:
        return True
    if error.result_code != INTERNAL_SERVER_ERROR_ARGS:
        return False
    normalized_message = error.result_message.casefold()
    return (
        "subworkflowdefinition" in normalized_message
        and "is not online" in normalized_message
    )


def _raise_workflow_run_error(
    error: ApiResultError,
    *,
    project_code: int,
    project_name: str,
    workflow: ResolvedWorkflow,
    retry_command: str = "dsctl workflow run",
) -> None:
    if _is_workflow_definition_not_online_run_error(error):
        message = f"Workflow '{workflow.name}' must be online before it can be run."
        raise InvalidStateError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "project": project_name,
                "project_code": project_code,
                "code": workflow.code,
                "name": workflow.name,
                "required_release_state": "ONLINE",
            },
            suggestion=(
                "Run `dsctl workflow online WORKFLOW --project PROJECT`, then "
                f"retry `{retry_command}`."
            ),
        ) from error
    raise error


def _is_workflow_definition_not_online_run_error(error: ApiResultError) -> bool:
    if error.result_code != START_WORKFLOW_INSTANCE_ERROR:
        return False
    compacted_message = "".join(
        character
        for character in error.result_message.casefold()
        if character.isalnum()
    )
    return (
        "workflowdefinitionshouldbeonline" in compacted_message
        or "workflowdefinitionnotonline" in compacted_message
    )


def _raise_workflow_create_error(
    error: ApiResultError,
    *,
    project: ResolvedProject,
    workflow_name: str,
) -> None:
    details: JsonObject = {
        "resource": WORKFLOW_RESOURCE,
        "project": project.name,
        "project_code": project.code,
        "name": workflow_name,
    }
    if error.result_code == PROJECT_NOT_FOUND:
        message = f"Project '{project.name}' was not found."
        raise NotFoundError(
            message,
            details=details,
        ) from error
    if error.result_code == WORKFLOW_DEFINITION_NAME_EXIST:
        message = (
            f"Workflow '{workflow_name}' already exists in project '{project.name}'."
        )
        raise ConflictError(
            message,
            details=details,
        ) from error
    if error.result_code in {
        TASK_NAME_DUPLICATE_ERROR,
        DATA_IS_NOT_VALID,
        WORKFLOW_NODE_HAS_CYCLE,
        WORKFLOW_NODE_S_PARAMETER_INVALID,
        TASK_DEFINE_NOT_EXIST,
        CHECK_WORKFLOW_TASK_RELATION_ERROR,
    }:
        raise UserInputError(
            error.result_message,
            details=details,
            suggestion=_WORKFLOW_CREATE_REVIEW_SUGGESTION,
        ) from error
    raise error


def _raise_workflow_update_error(
    error: ApiResultError,
    *,
    project: ResolvedProject,
    workflow: ResolvedWorkflow,
    workflow_name: str,
) -> None:
    details: JsonObject = {
        "resource": WORKFLOW_RESOURCE,
        "project": project.name,
        "project_code": project.code,
        "code": workflow.code,
        "name": workflow_name,
    }
    if error.result_code == WORKFLOW_DEFINITION_NOT_EXIST:
        message = f"Workflow '{workflow.name}' does not exist."
        raise NotFoundError(message, details=details) from error
    if error.result_code == WORKFLOW_DEFINITION_NAME_EXIST:
        message = (
            f"Workflow '{workflow_name}' already exists in project '{project.name}'."
        )
        raise ConflictError(message, details=details) from error
    if error.result_code == WORKFLOW_DEFINITION_NOT_ALLOWED_EDIT:
        message = f"Workflow '{workflow.name}' must be offline before it can be edited."
        raise InvalidStateError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl workflow offline WORKFLOW --project PROJECT` first, "
                "then retry `dsctl workflow edit`."
            ),
        ) from error
    if error.result_code in {
        TASK_NAME_DUPLICATE_ERROR,
        DATA_IS_NOT_VALID,
        WORKFLOW_NODE_HAS_CYCLE,
        WORKFLOW_NODE_S_PARAMETER_INVALID,
        TASK_DEFINE_NOT_EXIST,
        CHECK_WORKFLOW_TASK_RELATION_ERROR,
    }:
        raise UserInputError(
            error.result_message,
            details=details,
            suggestion=_WORKFLOW_EDIT_DRY_RUN_SUGGESTION,
        ) from error
    raise error


def _raise_workflow_delete_error(
    error: ApiResultError,
    *,
    project: ResolvedProject,
    workflow: ResolvedWorkflow,
) -> None:
    details: JsonObject = {
        "resource": WORKFLOW_RESOURCE,
        "project": project.name,
        "project_code": project.code,
        "code": workflow.code,
        "name": workflow.name,
    }
    if error.result_code == WORKFLOW_DEFINITION_NOT_EXIST:
        message = f"Workflow '{workflow.name}' does not exist."
        raise NotFoundError(message, details=details) from error
    if error.result_code == 30001:
        message = f"Current user cannot delete workflow '{workflow.name}'."
        raise PermissionDeniedError(message, details=details) from error
    if error.result_code == 50021:
        message = f"Workflow '{workflow.name}' must be offline before deletion."
        raise InvalidStateError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl workflow offline WORKFLOW --project PROJECT` first, "
                "then retry `dsctl workflow delete --force`."
            ),
        ) from error
    if error.result_code == 50023:
        message = (
            f"Workflow '{workflow.name}' still has an online schedule and cannot "
            "be deleted."
        )
        raise InvalidStateError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl schedule list --workflow WORKFLOW --project PROJECT` "
                "to find the attached schedule, take it offline with "
                "`dsctl schedule offline SCHEDULE_ID`, then retry "
                "`dsctl workflow delete --force`."
            ),
        ) from error
    if error.result_code == 10163:
        message = (
            f"Workflow '{workflow.name}' still has running workflow instances and "
            "cannot be deleted."
        )
        raise InvalidStateError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl workflow-instance list --workflow WORKFLOW --project "
                "PROJECT` to inspect active instances, stop or wait for them to "
                "finish, then retry deletion."
            ),
        ) from error
    if error.result_code == 10193:
        message = (
            f"Workflow '{workflow.name}' is still referenced by other tasks and "
            "cannot be deleted."
        )
        raise ConflictError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl workflow lineage dependent-tasks WORKFLOW --project "
                "PROJECT` to inspect references before retrying deletion."
            ),
        ) from error
    raise error


def _workflow_edit_release_state(patch: WorkflowPatchSpec) -> str | None:
    if patch.workflow is None:
        return None
    if "release_state" not in patch.workflow.set.model_fields_set:
        return None
    release_state = patch.workflow.set.release_state
    return None if release_state is None else release_state.value


def _workflow_edit_dry_run_result(
    *,
    project_code: int,
    workflow_code: int,
    payload: WorkflowUpdatePayload,
    resolved: JsonObject,
    diff: WorkflowPatchDiffData,
    workflow_state_constraints: list[str],
    workflow_state_constraint_details: list[WorkflowEditConstraintData],
    schedule_impacts: list[str],
    schedule_impact_details: list[WorkflowEditScheduleImpactData],
    no_change: bool,
    parameter_warnings: list[str],
    parameter_warning_details: list[ParameterExpressionWarningDetail],
) -> CommandResult:
    return dry_run_result(
        method="PUT",
        path=f"/projects/{project_code}/workflow-definition/{workflow_code}",
        form_data=require_json_object(
            payload,
            label="workflow update form data",
        ),
        resolved=resolved,
        extra_data=require_json_object(
            {
                "diff": require_json_value(diff, label="workflow edit diff"),
                "workflow_state_constraints": workflow_state_constraints,
                "workflow_state_constraint_details": workflow_state_constraint_details,
                "schedule_impacts": schedule_impacts,
                "schedule_impact_details": schedule_impact_details,
                "no_change": no_change,
            },
            label="workflow edit dry-run data",
        ),
        warnings=parameter_warnings,
        warning_details=_parameter_expression_warning_json_details(
            parameter_warning_details
        ),
    )


def _workflow_edit_state_constraint_details(
    workflow: WorkflowPayloadRecord,
    *,
    has_changes: bool,
) -> list[WorkflowEditConstraintData]:
    if not has_changes:
        return []
    if enum_value(workflow.releaseState) != "ONLINE":
        return []
    current_schedule_release_state = enum_value(workflow.scheduleReleaseState)
    constraints: list[WorkflowEditConstraintData] = [
        {
            "code": "workflow_must_be_offline",
            "message": (
                "workflow is currently online; DolphinScheduler only allows "
                "whole-definition edits while offline"
            ),
            "blocking": True,
            "current_release_state": "ONLINE",
            "required_release_state": "OFFLINE",
            "current_schedule_release_state": current_schedule_release_state,
        }
    ]
    if current_schedule_release_state == "ONLINE":
        constraints.append(
            {
                "code": "offline_also_offlines_attached_schedule",
                "message": (
                    "taking this workflow offline before apply will also take the "
                    "attached schedule offline"
                ),
                "blocking": False,
                "current_release_state": "ONLINE",
                "required_release_state": "OFFLINE",
                "current_schedule_release_state": current_schedule_release_state,
            }
        )
    return constraints


def _workflow_edit_schedule_impact_details(
    workflow: WorkflowPayloadRecord,
    *,
    desired_release_state: str | None,
) -> list[WorkflowEditScheduleImpactData]:
    if workflow.schedule is None:
        return []
    current_schedule_release_state = enum_value(workflow.scheduleReleaseState)
    impacts: list[WorkflowEditScheduleImpactData] = [
        {
            "code": "attached_schedule_not_modified",
            "message": (
                "workflow edit does not modify the attached schedule; use "
                "`schedule update|online|offline` separately"
            ),
            "desired_workflow_release_state": desired_release_state,
            "current_schedule_release_state": current_schedule_release_state,
        }
    ]
    if desired_release_state == "ONLINE" and current_schedule_release_state != "ONLINE":
        impacts.append(
            {
                "code": "workflow_online_leaves_schedule_offline",
                "message": (
                    "this edit can bring the workflow back online, but any attached "
                    "schedule remains offline until `schedule online` is requested"
                ),
                "desired_workflow_release_state": desired_release_state,
                "current_schedule_release_state": current_schedule_release_state,
            }
        )
    return impacts


def _workflow_edit_constraint_messages(
    constraints: list[WorkflowEditConstraintData],
) -> list[str]:
    return [constraint["message"] for constraint in constraints]


def _workflow_edit_schedule_impact_messages(
    impacts: list[WorkflowEditScheduleImpactData],
) -> list[str]:
    return [impact["message"] for impact in impacts]


def _workflow_edit_schedule_impact_warning_details(
    impacts: list[WorkflowEditScheduleImpactData],
) -> list[JsonObject]:
    return [
        require_json_object(
            impact,
            label="workflow edit schedule impact detail",
        )
        for impact in impacts
    ]


def _parameter_expression_warning_json_details(
    details: list[ParameterExpressionWarningDetail],
) -> list[JsonObject]:
    return [
        require_json_object(
            detail,
            label="parameter expression warning detail",
        )
        for detail in details
    ]


def _raise_workflow_edit_online_error(
    *,
    workflow: ResolvedWorkflow,
    workflow_state_constraint_details: list[WorkflowEditConstraintData],
) -> None:
    primary_constraint = workflow_state_constraint_details[0]
    details: JsonObject = {
        "resource": WORKFLOW_RESOURCE,
        "code": workflow.code,
        "name": workflow.name,
        "required_release_state": "OFFLINE",
        "current_release_state": "ONLINE",
        "constraint_detail": require_json_object(
            primary_constraint,
            label="workflow edit constraint detail",
        ),
    }
    if len(workflow_state_constraint_details) > 1:
        schedule_constraint = workflow_state_constraint_details[1]
        details["schedule_impact"] = schedule_constraint["message"]
        details["schedule_impact_detail"] = require_json_object(
            schedule_constraint,
            label="workflow edit schedule impact detail",
        )
    message = f"Workflow '{workflow.name}' must be offline before it can be edited."
    raise InvalidStateError(
        message,
        details=details,
        suggestion=(
            "Run `dsctl workflow offline WORKFLOW --project PROJECT` first, then "
            "retry `dsctl workflow edit`. Review `schedule_impact_detail` before "
            "taking an attached schedule offline."
        ),
    )


def _workflow_release_warnings(
    workflow: WorkflowPayloadRecord,
    *,
    action: Literal["online", "offline"],
) -> list[str]:
    schedule_release_state = enum_value(workflow.scheduleReleaseState)
    if action == "online":
        if workflow.schedule is not None and schedule_release_state != "ONLINE":
            return [
                "workflow brought online; any attached schedule remains offline "
                "until `schedule online` is requested"
            ]
        return []
    if schedule_release_state == "ONLINE":
        return ["workflow brought offline; any attached schedule is also taken offline"]
    return []


def _workflow_release_warning_details(
    workflow: WorkflowPayloadRecord,
    *,
    action: Literal["online", "offline"],
) -> list[JsonObject]:
    schedule_release_state = enum_value(workflow.scheduleReleaseState)
    workflow_release_state = enum_value(workflow.releaseState)
    if action == "online":
        if workflow.schedule is not None and schedule_release_state != "ONLINE":
            return [
                require_json_object(
                    WorkflowReleaseWarningDetail(
                        code="workflow_online_leaves_schedule_offline",
                        message=(
                            "workflow brought online; any attached schedule remains "
                            "offline until `schedule online` is requested"
                        ),
                        action=action,
                        workflow_release_state=workflow_release_state,
                        schedule_release_state=schedule_release_state,
                    ),
                    label="workflow release warning detail",
                )
            ]
        return []
    if schedule_release_state == "ONLINE":
        return [
            require_json_object(
                WorkflowReleaseWarningDetail(
                    code="workflow_offline_also_offlines_schedule",
                    message=(
                        "workflow brought offline; any attached schedule is also "
                        "taken offline"
                    ),
                    action=action,
                    workflow_release_state=workflow_release_state,
                    schedule_release_state=schedule_release_state,
                ),
                label="workflow release warning detail",
            )
        ]
    return []
