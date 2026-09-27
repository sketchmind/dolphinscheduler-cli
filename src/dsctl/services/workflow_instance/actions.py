from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from dsctl.cli_surface import WORKFLOW_INSTANCE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    InvalidStateError,
    UserInputError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._validation import (
    require_non_empty_text,
    require_positive_int,
)
from dsctl.services.runtime import (
    run_with_bound_domain_service_runtime,
)
from dsctl.upstream.mutation_outcomes import verify_mutation
from dsctl.upstream.replay_baselines import replay_baseline
from dsctl.upstream.resolver import task as resolve_task
from dsctl.upstream.runtime_enums import (
    WORKFLOW_EXECUTION_FAILURE_STATE,
    WORKFLOW_EXECUTION_STOP_STATE,
)
from dsctl.upstream.runtime_instances import (
    RUNTIME_INSTANCE_DOMAIN,
    LocatedWorkflowInstance,
    WorkflowInstanceSnapshot,
)
from dsctl.upstream.serialization import (
    enum_value,
    optional_text,
)

if TYPE_CHECKING:
    from dsctl.upstream.protocol import (
        StringEnumValue,
        TaskPayloadRecord,
    )


from dsctl.services.workflow_instance._commands import (
    _instance_workflow_run_command,
    _wait_for_final_state_suggestion,
    _workflow_instance_action_command,
    _workflow_instance_command,
)
from dsctl.services.workflow_instance._errors import (
    _raise_workflow_instance_action_error,
)
from dsctl.services.workflow_instance._selection import (
    _selected_project_data,
    _selected_workflow_instance,
    _workflow_execution_status,
    _workflow_instance_resolved,
    get_workflow_instance,
)
from dsctl.services.workflow_instance._types import (
    RuntimeInstanceServiceRuntime,
    WorkflowInstanceActionWarningDetail,
    WorkflowInstanceExecuteTaskResolved,
    WorkflowInstanceSelectionData,
    _InstanceDagTaskOperations,
)


def stop_workflow_instance_result(
    workflow_instance_id: int,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Request stop for one workflow instance and return the refreshed payload."""
    normalized_workflow_instance_id = require_positive_int(
        workflow_instance_id,
        label="workflow_instance_id",
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _stop_workflow_instance_result,
        workflow_instance_id=normalized_workflow_instance_id,
        project=optional_text(project),
    )


def rerun_workflow_instance_result(
    workflow_instance_id: int,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Request rerun for one finished workflow instance."""
    normalized_workflow_instance_id = require_positive_int(
        workflow_instance_id,
        label="workflow_instance_id",
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _rerun_workflow_instance_result,
        workflow_instance_id=normalized_workflow_instance_id,
        project=optional_text(project),
    )


def recover_failed_workflow_instance_result(
    workflow_instance_id: int,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Recover one failed workflow instance from failed tasks."""
    normalized_workflow_instance_id = require_positive_int(
        workflow_instance_id,
        label="workflow_instance_id",
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _recover_failed_workflow_instance_result,
        workflow_instance_id=normalized_workflow_instance_id,
        project=optional_text(project),
    )


def execute_task_in_workflow_instance_result(
    workflow_instance_id: int,
    *,
    project: str | None = None,
    task: str,
    scope: str = "self",
    env_file: str | None = None,
) -> CommandResult:
    """Execute one task within one finished workflow instance."""
    normalized_workflow_instance_id = require_positive_int(
        workflow_instance_id,
        label="workflow_instance_id",
    )
    normalized_task = require_non_empty_text(task, label="task")
    normalized_scope = _normalized_execute_task_scope(scope)
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _execute_task_in_workflow_instance_result,
        workflow_instance_id=normalized_workflow_instance_id,
        project=optional_text(project),
        task_identifier=normalized_task,
        scope=normalized_scope,
    )


def _stop_workflow_instance_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    workflow_instance_id: int,
    project: str | None,
) -> CommandResult:
    selected_project, located = _selected_workflow_instance(
        runtime,
        project=project,
        workflow_instance_id=workflow_instance_id,
    )
    payload = located.instance
    status = _workflow_execution_status(payload.state)
    state_name = enum_value(payload.state)
    if status is None or not status.can_stop:
        get_command = _workflow_instance_command(
            "get",
            workflow_instance_id=workflow_instance_id,
            project_selector=selected_project.value,
        )
        watch_command = _workflow_instance_command(
            "watch",
            workflow_instance_id=workflow_instance_id,
            project_selector=selected_project.value,
        )
        message = "This workflow instance cannot be stopped in its current state."
        raise InvalidStateError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
                "state": state_name,
            },
            suggestion=(
                f"Use `{get_command}` or `{watch_command}` to inspect the current "
                "state before retrying stop."
            ),
        )
    try:
        runtime.domain.instances.control_workflow_instance(
            LocatedWorkflowInstance(payload.project, payload),
            execute_type="STOP",
        )
    except ApiResultError as error:
        _raise_workflow_instance_action_error(
            error,
            baseline=payload,
            workflow_instance_id=workflow_instance_id,
            action="stop",
            project_selector=selected_project.value,
        )
    except ApiTransportError as error:
        _annotate_action_error(error, baseline=payload, action="stop", readback=False)
        raise
    refreshed_payload = _read_after_action(
        runtime,
        project_selector=selected_project.value,
        workflow_instance_id=workflow_instance_id,
        action="stop",
        baseline=payload,
    )
    warnings = _action_warning(
        "stop",
        refreshed_payload.state,
        expect_non_final=False,
        target_state=WORKFLOW_EXECUTION_STOP_STATE,
    )
    return CommandResult(
        data=require_json_object(
            refreshed_payload.to_data(),
            label="workflow-instance data",
        ),
        resolved=require_json_object(
            _workflow_instance_resolved(
                workflow_instance_id,
                project=refreshed_payload.project,
                selected_project=selected_project,
            ),
            label="workflow-instance resolved",
        ),
        warnings=warnings,
        warning_details=_action_warning_details(
            "stop",
            refreshed_payload.state,
            expect_non_final=False,
            target_state=WORKFLOW_EXECUTION_STOP_STATE,
        ),
    )


def _instance_kubeflow_tasks(
    payload: WorkflowInstanceSnapshot,
) -> tuple[TaskPayloadRecord, ...]:
    """Return KUBEFLOW tasks from the immutable instance DAG when available."""
    dag = payload.dagData
    if dag is None:
        return ()
    return tuple(
        task
        for task in (dag.taskDefinitionList or ())
        if (optional_text(task.taskType) or "").upper() == "KUBEFLOW"
    )


def _reject_kubeflow_same_instance_replay(
    payload: WorkflowInstanceSnapshot,
    *,
    action: Literal["rerun", "recover-failed", "execute-task"],
) -> None:
    """Reject controls when the available instance DAG reveals KUBEFLOW."""
    kubeflow_tasks = _instance_kubeflow_tasks(payload)
    if not kubeflow_tasks:
        return
    task_names = tuple(
        optional_text(task.name) or str(task.code) for task in kubeflow_tasks
    )
    message = (
        f"workflow-instance {action} cannot safely replay workflow instance "
        f"{payload.id} because its DAG contains KUBEFLOW task(s): "
        f"{', '.join(task_names)}"
    )
    command = _instance_workflow_run_command(payload)
    raise UserInputError(
        message,
        details={
            "resource": WORKFLOW_INSTANCE_RESOURCE,
            "id": payload.id,
            "action": action,
            "task_names": list(task_names),
            "reason": "kubeflow-workflow-instance-identity-reuse",
        },
        suggestion=(
            f"Start a new workflow instance with `{command}`. Same-instance "
            "rerun, recover-failed, and execute-task reuse "
            "system.workflow.instance.id and can re-observe an old terminal TFJob "
            "instead of starting fresh training."
        ),
    )


def _rerun_workflow_instance_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    workflow_instance_id: int,
    project: str | None,
) -> CommandResult:
    selected_project, located = _selected_workflow_instance(
        runtime,
        project=project,
        workflow_instance_id=workflow_instance_id,
    )
    payload = located.instance
    _reject_kubeflow_same_instance_replay(payload, action="rerun")
    status = _workflow_execution_status(payload.state)
    if status is None or not status.final_state:
        rerun_command = _workflow_instance_action_command(
            "rerun",
            workflow_instance_id=workflow_instance_id,
            project_selector=selected_project.value,
        )
        message = "This workflow instance must be in a final state before rerun."
        raise InvalidStateError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
                "state": enum_value(payload.state),
            },
            suggestion=_wait_for_final_state_suggestion(rerun_command),
        )
    try:
        runtime.domain.instances.control_workflow_instance(
            LocatedWorkflowInstance(payload.project, payload),
            execute_type="REPEAT_RUNNING",
        )
    except ApiResultError as exc:
        _raise_workflow_instance_action_error(
            exc,
            baseline=payload,
            workflow_instance_id=workflow_instance_id,
            action="rerun",
            project_selector=selected_project.value,
        )
    except ApiTransportError as error:
        _annotate_action_error(error, baseline=payload, action="rerun", readback=False)
        raise
    refreshed_payload = _read_after_action(
        runtime,
        project_selector=selected_project.value,
        workflow_instance_id=workflow_instance_id,
        action="rerun",
        baseline=payload,
    )
    result = CommandResult(
        data=require_json_object(
            refreshed_payload.to_data(),
            label="workflow-instance data",
        ),
        resolved=require_json_object(
            _workflow_instance_resolved(
                workflow_instance_id,
                project=refreshed_payload.project,
                selected_project=selected_project,
            ),
            label="workflow-instance resolved",
        ),
        warnings=_action_warning(
            "rerun",
            refreshed_payload.state,
            expect_non_final=True,
        ),
        warning_details=_action_warning_details(
            "rerun",
            refreshed_payload.state,
            expect_non_final=True,
        ),
    )
    baseline_data = replay_baseline(
        ds_version=payload.ds_version, action="rerun", run_times=payload.runTimes
    )
    if baseline_data is not None:
        result.resolved["execution_baseline"] = baseline_data
    return result


def _recover_failed_workflow_instance_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    workflow_instance_id: int,
    project: str | None,
) -> CommandResult:
    selected_project, located = _selected_workflow_instance(
        runtime,
        project=project,
        workflow_instance_id=workflow_instance_id,
    )
    payload = located.instance
    _reject_kubeflow_same_instance_replay(payload, action="recover-failed")
    status = _workflow_execution_status(payload.state)
    if status is None or status.value != WORKFLOW_EXECUTION_FAILURE_STATE:
        get_command = _workflow_instance_command(
            "get",
            workflow_instance_id=workflow_instance_id,
            project_selector=selected_project.value,
        )
        watch_command = _workflow_instance_command(
            "watch",
            workflow_instance_id=workflow_instance_id,
            project_selector=selected_project.value,
        )
        message = (
            "This workflow instance must be in FAILURE state before recover-failed."
        )
        raise InvalidStateError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
                "state": enum_value(payload.state),
            },
            suggestion=(
                f"Use `{get_command}` or `{watch_command}` to confirm "
                "the instance is in FAILURE before retrying `recover-failed`."
            ),
        )
    try:
        runtime.domain.instances.control_workflow_instance(
            LocatedWorkflowInstance(payload.project, payload),
            execute_type="START_FAILURE_TASK_PROCESS",
        )
    except ApiResultError as exc:
        _raise_workflow_instance_action_error(
            exc,
            baseline=payload,
            workflow_instance_id=workflow_instance_id,
            action="recover-failed",
            project_selector=selected_project.value,
        )
    except ApiTransportError as error:
        _annotate_action_error(
            error, baseline=payload, action="recover-failed", readback=False
        )
        raise
    refreshed_payload = _read_after_action(
        runtime,
        project_selector=selected_project.value,
        workflow_instance_id=workflow_instance_id,
        action="recover-failed",
        baseline=payload,
    )
    result = CommandResult(
        data=require_json_object(
            refreshed_payload.to_data(),
            label="workflow-instance data",
        ),
        resolved=require_json_object(
            _workflow_instance_resolved(
                workflow_instance_id,
                project=refreshed_payload.project,
                selected_project=selected_project,
            ),
            label="workflow-instance resolved",
        ),
        warnings=_action_warning(
            "recover-failed",
            refreshed_payload.state,
            expect_non_final=True,
        ),
        warning_details=_action_warning_details(
            "recover-failed",
            refreshed_payload.state,
            expect_non_final=True,
        ),
    )
    baseline_data = replay_baseline(
        ds_version=payload.ds_version,
        action="recover-failed",
        run_times=payload.runTimes,
    )
    if baseline_data is not None:
        result.resolved["execution_baseline"] = baseline_data
    return result


def _execute_task_in_workflow_instance_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    workflow_instance_id: int,
    project: str | None,
    task_identifier: str,
    scope: str,
) -> CommandResult:
    selected_project, located = _selected_workflow_instance(
        runtime,
        project=project,
        workflow_instance_id=workflow_instance_id,
    )
    payload = located.instance
    _reject_kubeflow_same_instance_replay(payload, action="execute-task")
    status = _workflow_execution_status(payload.state)
    if status is None or not status.final_state:
        execute_task_command = _workflow_instance_command(
            "execute-task",
            workflow_instance_id=workflow_instance_id,
            project_selector=selected_project.value,
            task=task_identifier,
        )
        message = "This workflow instance must be in a final state before execute-task."
        raise InvalidStateError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
                "state": enum_value(payload.state),
            },
            suggestion=_wait_for_final_state_suggestion(execute_task_command),
        )
    dag = payload.dagData
    if dag is None:
        message = "Workflow instance payload was missing dagData"
        raise ApiTransportError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
            },
        )
    resolved_task = resolve_task(
        task_identifier,
        adapter=_InstanceDagTaskOperations(dag),
        project_code=payload.projectCode or 0,
        workflow_code=payload.workflowDefinitionCode or 0,
    )
    try:
        runtime.domain.instances.execute_task(
            LocatedWorkflowInstance(payload.project, payload),
            task_code=resolved_task.code,
            scope=scope,
        )
    except ApiResultError as exc:
        _raise_workflow_instance_action_error(
            exc,
            baseline=payload,
            workflow_instance_id=workflow_instance_id,
            action="execute-task",
            task_code=resolved_task.code,
            project_selector=selected_project.value,
        )
    except ApiTransportError as error:
        _annotate_action_error(
            error, baseline=payload, action="execute-task", readback=False
        )
        raise
    refreshed_payload = _read_after_action(
        runtime,
        project_selector=selected_project.value,
        workflow_instance_id=workflow_instance_id,
        action="execute-task",
        baseline=payload,
    )
    result = CommandResult(
        data=require_json_object(
            refreshed_payload.to_data(),
            label="workflow-instance data",
        ),
        resolved=require_json_object(
            WorkflowInstanceExecuteTaskResolved(
                workflowInstance=WorkflowInstanceSelectionData(id=workflow_instance_id),
                project=_selected_project_data(
                    refreshed_payload.project, selected_project
                ),
                task=resolved_task.to_data(),
                scope=scope,
            ),
            label="workflow-instance execute-task resolved",
        ),
        warnings=_action_warning(
            "execute-task",
            refreshed_payload.state,
            expect_non_final=True,
        ),
        warning_details=_action_warning_details(
            "execute-task",
            refreshed_payload.state,
            expect_non_final=True,
        ),
    )
    baseline_data = replay_baseline(
        ds_version=payload.ds_version, action="execute-task", run_times=payload.runTimes
    )
    if baseline_data is not None:
        result.resolved["execution_baseline"] = baseline_data
    return result


def _action_warning(
    action: str,
    state: StringEnumValue | str | None,
    *,
    expect_non_final: bool,
    target_state: str | None = None,
) -> list[str]:
    detail = _action_warning_detail(
        action,
        state,
        expect_non_final=expect_non_final,
        target_state=target_state,
    )
    if detail is None:
        return []
    return [detail["message"]]


def _action_warning_details(
    action: str,
    state: StringEnumValue | str | None,
    *,
    expect_non_final: bool,
    target_state: str | None = None,
) -> list[WorkflowInstanceActionWarningDetail]:
    detail = _action_warning_detail(
        action,
        state,
        expect_non_final=expect_non_final,
        target_state=target_state,
    )
    if detail is None:
        return []
    return [detail]


def _action_warning_detail(
    action: str,
    state: StringEnumValue | str | None,
    *,
    expect_non_final: bool,
    target_state: str | None,
) -> WorkflowInstanceActionWarningDetail | None:
    status = _workflow_execution_status(state)
    current_state = enum_value(state) or "UNKNOWN"
    if expect_non_final:
        if status is not None and not status.final_state:
            return None
    elif target_state is not None and current_state == target_state:
        return None
    message = f"{action} requested; current workflow instance state is {current_state}"
    return WorkflowInstanceActionWarningDetail(
        code="workflow_instance_action_state_after_request",
        action=action,
        message=message,
        current_state=current_state,
        expect_non_final=expect_non_final,
        target_state=target_state,
    )


def _normalized_execute_task_scope(value: str) -> str:
    normalized = value.strip().lower()
    if normalized in {"self", "pre", "post"}:
        return normalized
    message = "Task execution scope must be one of: self, pre, post"
    raise UserInputError(
        message,
        details={"scope": value},
        suggestion="Pass `--scope self`, `--scope pre`, or `--scope post`.",
    )


def _read_after_action(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    project_selector: str,
    workflow_instance_id: int,
    action: str,
    baseline: WorkflowInstanceSnapshot,
) -> WorkflowInstanceSnapshot:
    try:
        return verify_mutation(
            lambda: get_workflow_instance(
                runtime,
                project_selector=project_selector,
                workflow_instance_id=workflow_instance_id,
            ),
            ds_version=baseline.ds_version,
            resource=WORKFLOW_INSTANCE_RESOURCE,
            operation=action,
        )
    except ApiTransportError as error:
        _annotate_action_error(error, baseline=baseline, action=action, readback=True)
        raise


def _annotate_action_error(
    error: ApiTransportError,
    *,
    baseline: WorkflowInstanceSnapshot,
    action: str,
    readback: bool,
) -> None:
    error.details["id"] = baseline.id
    error.details["project"] = baseline.project.to_data()
    error.details["known_resources"] = {
        "project": baseline.project.to_data(),
        "workflowInstance": {"id": baseline.id},
    }
    error.details["completed_stages"] = ["control_request"] if readback else []
    error.details["failed_stage"] = (
        "instance_readback" if readback else "control_request"
    )
    marker = replay_baseline(
        ds_version=baseline.ds_version, action=action, run_times=baseline.runTimes
    )
    if marker is not None:
        error.details["execution_baseline"] = marker
