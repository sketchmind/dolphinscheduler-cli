from __future__ import annotations

import json
import shlex
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal, cast

from dsctl.cli_surface import (
    TASK_RESOURCE,
    WORKFLOW_RESOURCE,
)
from dsctl.errors import (
    ApiResultError,
    InvalidStateError,
    UserInputError,
)
from dsctl.output import (
    CommandResult,
    dry_run_result,
    require_json_object,
)
from dsctl.services._schedule_environment_guard import (
    require_workflow_environment_inheritance,
)
from dsctl.services.runtime import (
    run_with_bound_domain_service_runtime,
)
from dsctl.services.selection import (
    selected_value_data,
)
from dsctl.upstream.serialization import (
    optional_text,
)
from dsctl.upstream.workflows import WORKFLOW_DOMAIN

if TYPE_CHECKING:
    from dsctl.support.yaml_io import JsonObject
    from dsctl.upstream.resolver import (
        ResolvedTask,
    )
    from dsctl.upstream.wire import WireRequest

from dsctl.services.workflow._errors import (
    _raise_workflow_run_error,
)
from dsctl.services.workflow._execution_options import (
    _load_workflow_project_preference_defaults,
    _selected_optional_int_data,
    _validate_workflow_backfill_inputs,
    _validate_workflow_start_param_names_for_profile,
    _workflow_backfill_settings,
    _workflow_run_settings,
    _workflow_run_start_params_for_profile,
    _WorkflowBackfillSettings,
    _WorkflowRunSettings,
)
from dsctl.services.workflow._graph import (
    _legacy_workflow_task_name,
    _load_legacy_workflow_graph,
)
from dsctl.services.workflow._mutation import (
    _require_workflow_wire_without_content,
    _workflow_wire_form,
    _workflow_wire_query,
)
from dsctl.services.workflow._selection import (
    _code_native_task,
    _resolve_workflow_target,
    _resolved_project_selection,
    _resolved_task_data,
    _resolved_workflow_selection,
    _ResolvedWorkflowTarget,
)
from dsctl.services.workflow._types import (
    WorkflowExecutionDryRunWarningDetail,
    WorkflowRunTaskDependencyWarningDetail,
    WorkflowRunTaskScope,
    WorkflowServiceRuntime,
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
    return run_with_bound_domain_service_runtime(
        env_file,
        WORKFLOW_DOMAIN,
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
    return run_with_bound_domain_service_runtime(
        env_file,
        WORKFLOW_DOMAIN,
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
    expected_parallelism_number: int | None = None,
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
    normalized_dates = [] if dates is None else dates
    normalized_params = [] if params is None else params
    validate_backfill_workflow_inputs(
        start=start,
        end=end,
        dates=normalized_dates,
        scope=scope,
        run_mode=run_mode,
        expected_parallelism_number=expected_parallelism_number,
        complement_dependent_mode=complement_dependent_mode,
        all_level_dependent=all_level_dependent,
        execution_order=execution_order,
        params=normalized_params,
    )
    normalized_task = optional_text(task)
    normalized_scope = _normalized_task_run_scope(scope)
    normalized_worker_group = optional_text(worker_group)
    normalized_tenant = optional_text(tenant)

    return run_with_bound_domain_service_runtime(
        env_file,
        WORKFLOW_DOMAIN,
        _backfill_workflow_result,
        workflow=workflow,
        project=project,
        start=start,
        end=end,
        dates=normalized_dates,
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
        params=normalized_params,
        dry_run=dry_run,
        execution_dry_run=execution_dry_run,
    )


def validate_backfill_workflow_inputs(
    *,
    start: str | None,
    end: str | None,
    dates: list[str],
    scope: str,
    run_mode: str | None,
    expected_parallelism_number: int | None,
    complement_dependent_mode: str | None,
    all_level_dependent: bool,
    execution_order: str | None,
    params: list[str],
) -> None:
    """Validate local backfill syntax before configuration or version discovery."""
    _normalized_task_run_scope(scope)
    _validate_workflow_backfill_inputs(
        start=start,
        end=end,
        dates=dates,
        run_mode=run_mode,
        expected_parallelism_number=expected_parallelism_number,
        complement_dependent_mode=complement_dependent_mode,
        all_level_dependent=all_level_dependent,
        execution_order=execution_order,
        params=params,
    )


def _run_workflow_result(
    runtime: WorkflowServiceRuntime,
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
    runtime.domain.workflows.require_execution_options(
        action="workflow.run",
        tenant_code=tenant,
        environment_code=environment_code,
        execution_dry_run=execution_dry_run,
    )
    start_params = _workflow_run_start_params_for_profile(
        runtime.domain.workflows,
        params=params,
        action="workflow.run",
    )
    target = _resolve_workflow_target(
        runtime,
        workflow=workflow,
        project=project,
        action="workflow.run",
    )
    _require_online_workflow_execution(target, action="workflow.run")
    _validate_workflow_start_param_names_for_profile(
        runtime.domain.workflows,
        target=target,
        start_param_names=start_params[1],
        action="workflow.run",
    )
    project_preference = _load_workflow_project_preference_defaults(
        runtime,
        target=target,
    )
    settings = _workflow_run_settings(
        worker_group=worker_group,
        tenant=tenant,
        failure_strategy=failure_strategy,
        priority=priority,
        warning_type=warning_type,
        warning_group_id=warning_group_id,
        environment_code=environment_code,
        start_params=start_params,
        project_preference=project_preference,
        execution_dry_run=execution_dry_run,
    )
    require_workflow_environment_inheritance(
        runtime.profile.ds_version, settings.environment_code.value
    )
    resolved = _workflow_run_resolved(target, settings)
    prepared = runtime.domain.workflows.prepare_execution(
        target.scope,
        schedule_time=_workflow_run_start_process_schedule_time(
            runtime.domain.workflows.execution_schedule_time_shape,
        ),
        command_type="START_PROCESS",
        worker_group=settings.worker_group.value,
        tenant_code=settings.tenant.value,
        failure_strategy=settings.failure_strategy.value,
        warning_type=settings.warning_type.value,
        workflow_instance_priority=settings.workflow_instance_priority.value,
        warning_group_id=settings.warning_group_id.value,
        environment_code=settings.environment_code.value,
        start_params=settings.start_params,
        execution_dry_run=settings.execution_dry_run,
    )
    if dry_run:
        return _workflow_execution_dry_run_result(
            request=prepared.request,
            settings=settings,
            resolved=resolved,
        )
    try:
        receipt = runtime.domain.workflows.apply_execution(prepared)
    except ApiResultError as error:
        _raise_workflow_run_error(
            error,
            project=target.project,
            workflow=target.workflow,
        )
    data = require_json_object(
        receipt.to_data(),
        label="workflow run data",
    )
    return CommandResult(
        data=data,
        resolved=resolved,
        warnings=_workflow_run_warnings(settings),
        warning_details=_workflow_run_warning_details(settings),
    )


def _run_workflow_task_result(
    runtime: WorkflowServiceRuntime,
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
    runtime.domain.workflows.require_execution_options(
        action="workflow.run-task",
        tenant_code=tenant,
        environment_code=environment_code,
        execution_dry_run=execution_dry_run,
    )
    start_params = _workflow_run_start_params_for_profile(
        runtime.domain.workflows,
        params=params,
        action="workflow.run-task",
    )
    target = _resolve_workflow_target(
        runtime,
        workflow=workflow,
        project=project,
        action="workflow.run-task",
    )
    _require_online_workflow_execution(target, action="workflow.run-task")
    _validate_workflow_start_param_names_for_profile(
        runtime.domain.workflows,
        target=target,
        start_param_names=start_params[1],
        action="workflow.run-task",
    )
    if runtime.domain.workflows.workflow_graph_family == "legacy-json":
        return _run_legacy_workflow_task_result(
            runtime,
            target=target,
            task_name=task,
            scope=scope,
            worker_group=worker_group,
            tenant=tenant,
            failure_strategy=failure_strategy,
            priority=priority,
            warning_type=warning_type,
            warning_group_id=warning_group_id,
            environment_code=environment_code,
            start_params=start_params,
            dry_run=dry_run,
            execution_dry_run=execution_dry_run,
        )
    resolved_task_payload = runtime.domain.workflows.resolve_task(
        target.scope,
        task,
        action="workflow.run-task",
    )
    resolved_task = _code_native_task(resolved_task_payload)
    project_preference = _load_workflow_project_preference_defaults(
        runtime,
        target=target,
    )
    settings = _workflow_run_settings(
        worker_group=worker_group,
        tenant=tenant,
        failure_strategy=failure_strategy,
        priority=priority,
        warning_type=warning_type,
        warning_group_id=warning_group_id,
        environment_code=environment_code,
        start_params=start_params,
        project_preference=project_preference,
        execution_dry_run=execution_dry_run,
    )
    require_workflow_environment_inheritance(
        runtime.profile.ds_version, settings.environment_code.value
    )
    resolved = _workflow_run_resolved(target, settings, task=resolved_task, scope=scope)
    dependency_warning = _workflow_run_task_dependency_warning(scope=scope)
    dependency_warning_detail = _workflow_run_task_dependency_warning_detail(
        scope=scope,
        message=dependency_warning,
    )
    prepared = runtime.domain.workflows.prepare_execution(
        target.scope,
        schedule_time=_workflow_run_start_process_schedule_time(
            runtime.domain.workflows.execution_schedule_time_shape,
        ),
        command_type="START_PROCESS",
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
        execution_dry_run=settings.execution_dry_run,
    )
    if dry_run:
        return _workflow_execution_dry_run_result(
            request=prepared.request,
            settings=settings,
            resolved=resolved,
            warnings=[dependency_warning],
            warning_details=[dependency_warning_detail],
        )
    try:
        receipt = runtime.domain.workflows.apply_execution(prepared)
    except ApiResultError as error:
        _raise_workflow_run_error(
            error,
            project=target.project,
            workflow=target.workflow,
            operation="workflow.run-task",
            retry_command="dsctl workflow run-task WORKFLOW --task TASK",
        )
    data = require_json_object(
        receipt.to_data(),
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


def _run_legacy_workflow_task_result(
    runtime: WorkflowServiceRuntime,
    *,
    target: _ResolvedWorkflowTarget,
    task_name: str,
    scope: WorkflowRunTaskScope,
    worker_group: str | None,
    tenant: str | None,
    failure_strategy: str | None,
    priority: str | None,
    warning_type: str | None,
    warning_group_id: int | None,
    environment_code: int | None,
    start_params: tuple[str | None, list[str]],
    dry_run: bool,
    execution_dry_run: bool,
) -> CommandResult:
    """Run a DS 1.3.9 task by canonical name through its exact executor."""
    _snapshot, graph = _load_legacy_workflow_graph(
        runtime,
        target=target,
        action="workflow.run-task",
    )
    selected_task_name = _legacy_workflow_task_name(
        graph,
        target=target,
        selector=task_name,
    )
    project_preference = _load_workflow_project_preference_defaults(
        runtime,
        target=target,
    )
    settings = _workflow_run_settings(
        worker_group=worker_group,
        tenant=tenant,
        failure_strategy=failure_strategy,
        priority=priority,
        warning_type=warning_type,
        warning_group_id=warning_group_id,
        environment_code=environment_code,
        start_params=start_params,
        project_preference=project_preference,
        execution_dry_run=execution_dry_run,
    )
    require_workflow_environment_inheritance(
        runtime.profile.ds_version, settings.environment_code.value
    )
    resolved = _workflow_run_resolved(
        target,
        settings,
        selected_task_name=selected_task_name,
        scope=scope,
    )
    dependency_warning = _workflow_run_task_dependency_warning(scope=scope)
    dependency_warning_detail = _workflow_run_task_dependency_warning_detail(
        scope=scope,
        message=dependency_warning,
    )
    prepared = runtime.domain.workflows.prepare_execution(
        target.scope,
        schedule_time=_workflow_run_start_process_schedule_time(
            runtime.domain.workflows.execution_schedule_time_shape,
        ),
        command_type="START_PROCESS",
        worker_group=settings.worker_group.value,
        tenant_code=settings.tenant.value,
        start_node_list=[selected_task_name],
        task_scope=scope,
        failure_strategy=settings.failure_strategy.value,
        warning_type=settings.warning_type.value,
        workflow_instance_priority=settings.workflow_instance_priority.value,
        warning_group_id=settings.warning_group_id.value,
        environment_code=settings.environment_code.value,
        start_params=settings.start_params,
        execution_dry_run=settings.execution_dry_run,
    )
    warnings = [*_workflow_run_warnings(settings), dependency_warning]
    warning_details = [
        *_workflow_run_warning_details(settings),
        dependency_warning_detail,
    ]
    if dry_run:
        request = prepared.request
        _require_workflow_wire_without_content(
            request,
            label="workflow run-task request",
        )
        return dry_run_result(
            method=request.method,
            path=request.path,
            params=_workflow_wire_query(
                request,
                label="workflow run-task query",
            ),
            json_body=request.json,
            form_data=_workflow_wire_form(
                request,
                label="workflow run-task form",
            ),
            resolved=resolved,
            warnings=warnings,
            warning_details=warning_details,
        )
    try:
        receipt = runtime.domain.workflows.apply_execution(prepared)
    except ApiResultError as error:
        _raise_workflow_run_error(
            error,
            project=target.project,
            workflow=target.workflow,
            operation="workflow.run-task",
            retry_command="dsctl workflow run-task WORKFLOW --task TASK",
        )
    data = require_json_object(
        receipt.to_data(),
        label="workflow run-task data",
    )
    return CommandResult(
        data=data,
        resolved=resolved,
        warnings=warnings,
        warning_details=warning_details,
    )


def _backfill_workflow_result(
    runtime: WorkflowServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
    start: str | None,
    end: str | None,
    dates: list[str],
    task: str | None,
    scope: WorkflowRunTaskScope,
    run_mode: str | None,
    expected_parallelism_number: int | None,
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
    operations = runtime.domain.workflows
    operations.require_execution_options(
        action="workflow.backfill",
        tenant_code=tenant,
        environment_code=environment_code,
        execution_dry_run=execution_dry_run,
        run_mode=run_mode,
        expected_parallelism_number=expected_parallelism_number,
        complement_dependent_mode=complement_dependent_mode,
        all_level_dependent=all_level_dependent,
        execution_order=execution_order,
    )
    start_params = _workflow_run_start_params_for_profile(
        operations,
        params=params,
        action="workflow.backfill",
    )
    operations.require_action("workflow.backfill")
    if task is not None:
        operations.require_action("workflow.run-task")
    normalized_parallelism = operations.normalize_expected_parallelism_number(
        expected_parallelism_number
    )
    target = _resolve_workflow_target(
        runtime,
        workflow=workflow,
        project=project,
        action="workflow.backfill" if task is None else "workflow.run-task",
    )
    _require_online_workflow_execution(target, action="workflow.backfill")
    _validate_workflow_start_param_names_for_profile(
        operations,
        target=target,
        start_param_names=start_params[1],
        action="workflow.backfill",
    )
    legacy_task_name: str | None = None
    resolved_task: ResolvedTask | None = None
    if task is not None:
        if operations.workflow_graph_family == "legacy-json":
            _snapshot, graph = _load_legacy_workflow_graph(
                runtime,
                target=target,
                action="workflow.run-task",
            )
            legacy_task_name = _legacy_workflow_task_name(
                graph,
                target=target,
                selector=task,
            )
        else:
            resolved_task = _code_native_task(
                operations.resolve_task(
                    target.scope,
                    task,
                    action="workflow.run-task",
                )
            )
    project_preference = _load_workflow_project_preference_defaults(
        runtime,
        target=target,
    )
    settings = _workflow_run_settings(
        worker_group=worker_group,
        tenant=tenant,
        failure_strategy=failure_strategy,
        priority=priority,
        warning_type=warning_type,
        warning_group_id=warning_group_id,
        environment_code=environment_code,
        start_params=start_params,
        project_preference=project_preference,
        execution_dry_run=execution_dry_run,
    )
    require_workflow_environment_inheritance(
        runtime.profile.ds_version, settings.environment_code.value
    )
    backfill_settings = _workflow_backfill_settings(
        start=start,
        end=end,
        dates=dates,
        run_mode=run_mode,
        expected_parallelism_number=normalized_parallelism,
        complement_dependent_mode=complement_dependent_mode,
        all_level_dependent=all_level_dependent,
        execution_order=execution_order,
        execution_schedule_time_shape=operations.execution_schedule_time_shape,
        ds_version=operations.ds_version,
    )
    start_node_list: list[int] | list[str] | None
    if legacy_task_name is not None:
        start_node_list = [legacy_task_name]
    elif resolved_task is not None:
        start_node_list = [resolved_task.code]
    else:
        start_node_list = None
    task_scope = None if start_node_list is None else scope
    resolved = _workflow_run_resolved(
        target,
        settings,
        task=resolved_task,
        selected_task_name=legacy_task_name,
        scope=task_scope,
        backfill_settings=backfill_settings,
    )
    dependency_warning = (
        None
        if start_node_list is None
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
    prepared = operations.prepare_execution(
        target.scope,
        schedule_time=backfill_settings.schedule_time,
        command_type="COMPLEMENT_DATA",
        run_mode=backfill_settings.run_mode.value,
        expected_parallelism_number=backfill_settings.expected_parallelism_number,
        complement_dependent_mode=backfill_settings.complement_dependent_mode.value,
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
        execution_dry_run=settings.execution_dry_run,
    )
    if dry_run:
        return _workflow_execution_dry_run_result(
            request=prepared.request,
            settings=settings,
            resolved=resolved,
            warnings=extra_warnings,
            warning_details=extra_warning_details,
        )
    try:
        receipt = operations.apply_execution(prepared)
    except ApiResultError as error:
        _raise_workflow_run_error(
            error,
            project=target.project,
            workflow=target.workflow,
            operation="workflow.backfill",
            partial_dispatch_possible=(
                backfill_settings.run_mode.value == "RUN_MODE_PARALLEL"
            ),
            retry_command="dsctl workflow backfill WORKFLOW --start START --end END",
        )
    data = require_json_object(
        receipt.to_data(),
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


def _require_online_workflow_execution(
    target: _ResolvedWorkflowTarget,
    *,
    action: str,
) -> None:
    """Reject a known non-online definition before preparing an execution."""
    release_state = target.scope.view.release_state
    if release_state is None or release_state.upper() == "ONLINE":
        return
    workflow_name = target.workflow.name or str(target.workflow.native.value)
    online_command = shlex.join(
        [
            "dsctl",
            "workflow",
            "online",
            target.selected_workflow.value,
            "--project",
            target.selected_project.value,
        ]
    )
    message = f"Workflow {workflow_name!r} must be online before it can be run."
    raise InvalidStateError(
        message,
        details={
            "resource": WORKFLOW_RESOURCE,
            "action": action,
            "project": target.project.name,
            "workflow": target.workflow.name,
            "current_release_state": release_state,
            "required_release_state": "ONLINE",
        },
        suggestion=f"Run `{online_command}`, then retry the original command.",
    )


def _workflow_run_resolved(
    target: _ResolvedWorkflowTarget,
    settings: _WorkflowRunSettings,
    *,
    task: ResolvedTask | None = None,
    selected_task_name: str | None = None,
    scope: WorkflowRunTaskScope | None = None,
    backfill_settings: _WorkflowBackfillSettings | None = None,
) -> JsonObject:
    resolved: JsonObject = {
        "project": _resolved_project_selection(
            target.project,
            target.selected_project,
        ),
        "workflow": _resolved_workflow_selection(
            target.workflow,
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
    if task is not None and selected_task_name is not None:
        message = "workflow run resolution received two task identities"
        raise RuntimeError(message)
    if task is not None:
        resolved["task"] = _resolved_task_data(task)
    if selected_task_name is not None:
        resolved["task"] = {"name": selected_task_name}
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


def _workflow_execution_dry_run_result(
    *,
    request: WireRequest,
    settings: _WorkflowRunSettings,
    resolved: JsonObject,
    warnings: list[str] | None = None,
    warning_details: list[JsonObject] | None = None,
) -> CommandResult:
    """Render the exact prepared executor request used by apply."""
    _require_workflow_wire_without_content(
        request,
        label="workflow execution request",
    )
    return dry_run_result(
        method=request.method,
        path=request.path,
        params=_workflow_wire_query(request, label="workflow execution query"),
        json_body=request.json,
        form_data=_workflow_wire_form(request, label="workflow execution form"),
        resolved=resolved,
        warnings=[*_workflow_run_warnings(settings), *(warnings or [])],
        warning_details=[
            *_workflow_run_warning_details(settings),
            *(warning_details or []),
        ],
    )


def _workflow_run_start_process_schedule_time(
    shape: Literal["comma-range", "json"],
) -> str:
    timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
    if shape == "comma-range":
        return f"{timestamp},{timestamp}"
    return json.dumps(
        {
            "complementStartDate": timestamp,
            "complementEndDate": timestamp,
        },
        ensure_ascii=True,
        separators=(",", ":"),
    )


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
