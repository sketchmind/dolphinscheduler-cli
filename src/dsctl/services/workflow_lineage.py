from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict, cast

from dsctl.errors import ApiResultError, ApiTransportError, InvalidStateError
from dsctl.output import CommandResult, require_json_object, require_json_value
from dsctl.services.runtime import (
    BoundDomainServiceRuntime,
    run_with_bound_domain_service_runtime,
)
from dsctl.services.selection import (
    SelectedValue,
    require_project_selection,
    require_workflow_selection,
    with_selection_source,
)
from dsctl.upstream.definition_models import (
    NativeCode,
    NativeIdentity,
    ProjectRef,
    WorkflowRef,
)
from dsctl.upstream.serialization import (
    serialize_dependent_lineage_task,
    serialize_workflow_lineage,
)
from dsctl.upstream.workflows import WORKFLOW_DOMAIN, WorkflowDomain

if TYPE_CHECKING:
    from dsctl.services.selection import SelectionData
    from dsctl.upstream.protocol import TaskPayloadRecord

QUERY_WORKFLOW_LINEAGE_ERROR = 10161
WORKFLOW_LINEAGE_RESOURCE = "workflow-lineage"


class WorkflowLineageErrorDetails(TypedDict, total=False):
    """Structured error details for workflow-lineage query failures."""

    project_code: int
    project_name: str | None
    workflow_code: int
    workflow_name: str | None
    result_code: int


def list_workflow_lineage_result(
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Return the workflow-lineage graph for one selected project."""
    return run_with_bound_domain_service_runtime(
        env_file,
        WORKFLOW_DOMAIN,
        _list_workflow_lineage_result,
        project=project,
    )


def get_workflow_lineage_result(
    workflow: str | None,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Return the workflow-lineage graph for one selected workflow."""
    return run_with_bound_domain_service_runtime(
        env_file,
        WORKFLOW_DOMAIN,
        _get_workflow_lineage_result,
        workflow=workflow,
        project=project,
    )


def list_workflow_dependent_tasks_result(
    workflow: str | None,
    *,
    task: str | None = None,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Return dependent workflows/tasks for one workflow or task."""
    return run_with_bound_domain_service_runtime(
        env_file,
        WORKFLOW_DOMAIN,
        _list_workflow_dependent_tasks_result,
        workflow=workflow,
        task=task,
        project=project,
    )


def _list_workflow_lineage_result(
    runtime: BoundDomainServiceRuntime[WorkflowDomain],
    *,
    project: str | None,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    operations = runtime.domain.workflows
    operations.require_action("workflow.lineage.list")
    resolved_project = operations.resolve_project(selected_project.value)
    try:
        lineage = operations.lineage_list_resolved(resolved_project)
    except ApiResultError as exc:
        raise _translate_lineage_error(
            exc,
            project=resolved_project,
            workflow=None,
        ) from exc
    return CommandResult(
        data=require_json_object(
            serialize_workflow_lineage(lineage),
            label="workflow lineage data",
        ),
        resolved={
            "project": _resolved_project_selection(
                resolved_project,
                selected_project,
            )
        },
    )


def _get_workflow_lineage_result(
    runtime: BoundDomainServiceRuntime[WorkflowDomain],
    *,
    workflow: str | None,
    project: str | None,
) -> CommandResult:
    selected_project, selected_workflow = _selected_workflow_target(
        runtime,
        workflow=workflow,
        project=project,
    )
    operations = runtime.domain.workflows
    operations.require_action("workflow.lineage.get")
    scope = operations.resolve_workflow(
        selected_project.value,
        selected_workflow.value,
    )
    try:
        lineage = operations.lineage_get_resolved(scope)
    except ApiResultError as exc:
        raise _translate_lineage_error(
            exc,
            project=scope.project,
            workflow=scope.workflow,
        ) from exc
    return CommandResult(
        data=require_json_object(
            serialize_workflow_lineage(lineage),
            label="workflow lineage data",
        ),
        resolved={
            "project": _resolved_project_selection(
                scope.project,
                selected_project,
            ),
            "workflow": _resolved_workflow_selection(
                scope.workflow,
                selected_workflow,
            ),
        },
    )


def _list_workflow_dependent_tasks_result(
    runtime: BoundDomainServiceRuntime[WorkflowDomain],
    *,
    workflow: str | None,
    task: str | None,
    project: str | None,
) -> CommandResult:
    selected_project, selected_workflow = _selected_workflow_target(
        runtime,
        workflow=workflow,
        project=project,
    )
    selected_task = None if task is None else SelectedValue(value=task, source="flag")
    operations = runtime.domain.workflows
    action = "workflow.lineage.dependent-tasks"
    operations.require_action(action)
    scope = operations.resolve_workflow(
        selected_project.value,
        selected_workflow.value,
    )
    try:
        resolved_task, dependent_tasks = operations.dependent_tasks_resolved(
            scope,
            task_selector=(None if selected_task is None else selected_task.value),
        )
    except ApiResultError as exc:
        raise _translate_lineage_error(
            exc,
            project=scope.project,
            workflow=scope.workflow,
        ) from exc
    return CommandResult(
        data=require_json_value(
            [
                serialize_dependent_lineage_task(task_item)
                for task_item in dependent_tasks
            ],
            label="workflow dependent tasks data",
        ),
        resolved=require_json_object(
            _dependent_tasks_resolved_payload(
                project=scope.project,
                workflow=scope.workflow,
                selected_project=selected_project,
                selected_workflow=selected_workflow,
                selected_task=selected_task,
                resolved_task=resolved_task,
            ),
            label="workflow dependent tasks resolved",
        ),
    )


def _selected_workflow_target(
    runtime: BoundDomainServiceRuntime[WorkflowDomain],
    *,
    workflow: str | None,
    project: str | None,
) -> tuple[SelectedValue, SelectedValue]:
    selected_project = require_project_selection(project, runtime=runtime)
    selected_workflow = require_workflow_selection(
        workflow,
        input_form="argument",
    )
    return selected_project, selected_workflow


def _translate_lineage_error(
    error: ApiResultError,
    *,
    project: ProjectRef,
    workflow: WorkflowRef | None,
) -> ApiResultError | InvalidStateError:
    if error.result_code != QUERY_WORKFLOW_LINEAGE_ERROR:
        return error
    details: WorkflowLineageErrorDetails = {
        "project_code": _required_native_code(project.native, label="project"),
        "project_name": project.name,
        "result_code": error.result_code,
    }
    if workflow is not None:
        details["workflow_code"] = _required_native_code(
            workflow.native,
            label="workflow",
        )
        details["workflow_name"] = workflow.name
        message = f"Workflow lineage query failed for workflow '{workflow.name}'."
    else:
        message = f"Workflow lineage query failed for project '{project.name}'."
    return InvalidStateError(
        message,
        details=details,
        source=error.to_payload(),
        suggestion=(
            "Verify the selected workflow graph and dependent/sub-workflow "
            "references, then retry."
        ),
    )


def _resolved_project_selection(
    project: ProjectRef,
    selection: SelectedValue,
) -> SelectionData:
    return with_selection_source(cast("SelectionData", project.to_data()), selection)


def _resolved_workflow_selection(
    workflow: WorkflowRef,
    selection: SelectedValue,
) -> SelectionData:
    return with_selection_source(cast("SelectionData", workflow.to_data()), selection)


def _resolved_task_selection(
    task: TaskPayloadRecord,
    selection: SelectedValue,
) -> SelectionData:
    return with_selection_source(
        cast(
            "SelectionData",
            {"code": task.code, "name": task.name, "version": task.version},
        ),
        selection,
    )


def _dependent_tasks_resolved_payload(
    *,
    project: ProjectRef,
    workflow: WorkflowRef,
    selected_project: SelectedValue,
    selected_workflow: SelectedValue,
    selected_task: SelectedValue | None,
    resolved_task: TaskPayloadRecord | None,
) -> dict[str, SelectionData]:
    resolved: dict[str, SelectionData] = {
        "project": _resolved_project_selection(
            project,
            selected_project,
        ),
        "workflow": _resolved_workflow_selection(
            workflow,
            selected_workflow,
        ),
    }
    if selected_task is not None and resolved_task is not None:
        resolved["task"] = _resolved_task_selection(resolved_task, selected_task)
    return resolved


def _required_native_code(native: NativeIdentity, *, label: str) -> int:
    if isinstance(native, NativeCode):
        return native.value
    message = f"Workflow lineage returned a non-code-native {label}"
    raise ApiTransportError(
        message,
        details={"resource": WORKFLOW_LINEAGE_RESOURCE},
    )
