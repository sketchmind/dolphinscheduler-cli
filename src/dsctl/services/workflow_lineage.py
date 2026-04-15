from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, TypedDict, cast

from dsctl.errors import ApiResultError, InvalidStateError
from dsctl.output import CommandResult, require_json_object, require_json_value
from dsctl.services._serialization import (
    serialize_dependent_lineage_task,
    serialize_workflow_lineage,
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
    with_selection_source,
)

if TYPE_CHECKING:
    from dsctl.services.selection import SelectionData
    from dsctl.upstream.protocol import (
        DependentLineageTaskRecord,
        WorkflowLineageRecord,
    )

QUERY_WORKFLOW_LINEAGE_ERROR = 10161


class WorkflowLineageErrorDetails(TypedDict, total=False):
    """Structured error details for workflow-lineage query failures."""

    project_code: int
    project_name: str | None
    workflow_code: int
    workflow_name: str | None
    result_code: int


@dataclass(frozen=True)
class _ResolvedWorkflowLineageTarget:
    """One fully resolved project/workflow target for lineage read operations."""

    selected_project: SelectedValue
    resolved_project: ResolvedProject
    selected_workflow: SelectedValue
    resolved_workflow: ResolvedWorkflow


def list_workflow_lineage_result(
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Return the workflow-lineage graph for one selected project."""
    return run_with_service_runtime(
        env_file,
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
    return run_with_service_runtime(
        env_file,
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
    return run_with_service_runtime(
        env_file,
        _list_workflow_dependent_tasks_result,
        workflow=workflow,
        task=task,
        project=project,
    )


def _list_workflow_lineage_result(
    runtime: ServiceRuntime,
    *,
    project: str | None,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    lineage = _load_project_lineage(
        runtime,
        project=resolved_project,
    )
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
    runtime: ServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
) -> CommandResult:
    target = _resolve_workflow_lineage_target(
        runtime,
        workflow=workflow,
        project=project,
    )
    lineage = _load_workflow_lineage(runtime, target=target)
    return CommandResult(
        data=require_json_object(
            serialize_workflow_lineage(lineage),
            label="workflow lineage data",
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


def _list_workflow_dependent_tasks_result(
    runtime: ServiceRuntime,
    *,
    workflow: str | None,
    task: str | None,
    project: str | None,
) -> CommandResult:
    target = _resolve_workflow_lineage_target(
        runtime,
        workflow=workflow,
        project=project,
    )
    selected_task = None if task is None else SelectedValue(value=task, source="flag")
    resolved_task = (
        None
        if selected_task is None
        else resolve_task(
            selected_task.value,
            adapter=runtime.upstream.tasks,
            project_code=target.resolved_project.code,
            workflow_code=target.resolved_workflow.code,
        )
    )
    dependent_tasks = _load_dependent_tasks(
        runtime,
        target=target,
        task=resolved_task,
    )
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
                target=target,
                selected_task=selected_task,
                resolved_task=resolved_task,
            ),
            label="workflow dependent tasks resolved",
        ),
    )


def _resolve_workflow_lineage_target(
    runtime: ServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
) -> _ResolvedWorkflowLineageTarget:
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
    return _ResolvedWorkflowLineageTarget(
        selected_project=selected_project,
        resolved_project=resolved_project,
        selected_workflow=selected_workflow,
        resolved_workflow=resolved_workflow,
    )


def _load_project_lineage(
    runtime: ServiceRuntime,
    *,
    project: ResolvedProject,
) -> WorkflowLineageRecord | None:
    try:
        return runtime.upstream.workflow_lineages.list(project_code=project.code)
    except ApiResultError as exc:
        raise _translate_lineage_error(exc, project=project, workflow=None) from exc


def _load_workflow_lineage(
    runtime: ServiceRuntime,
    *,
    target: _ResolvedWorkflowLineageTarget,
) -> WorkflowLineageRecord | None:
    try:
        return runtime.upstream.workflow_lineages.get(
            project_code=target.resolved_project.code,
            workflow_code=target.resolved_workflow.code,
        )
    except ApiResultError as exc:
        raise _translate_lineage_error(
            exc,
            project=target.resolved_project,
            workflow=target.resolved_workflow,
        ) from exc


def _load_dependent_tasks(
    runtime: ServiceRuntime,
    *,
    target: _ResolvedWorkflowLineageTarget,
    task: ResolvedTask | None,
) -> list[DependentLineageTaskRecord]:
    try:
        return list(
            runtime.upstream.workflow_lineages.query_dependent_tasks(
                project_code=target.resolved_project.code,
                workflow_code=target.resolved_workflow.code,
                task_code=None if task is None else task.code,
            )
        )
    except ApiResultError as exc:
        raise _translate_lineage_error(
            exc,
            project=target.resolved_project,
            workflow=target.resolved_workflow,
        ) from exc


def _translate_lineage_error(
    error: ApiResultError,
    *,
    project: ResolvedProject,
    workflow: ResolvedWorkflow | None,
) -> ApiResultError | InvalidStateError:
    if error.result_code != QUERY_WORKFLOW_LINEAGE_ERROR:
        return error
    details: WorkflowLineageErrorDetails = {
        "project_code": project.code,
        "project_name": project.name,
        "result_code": error.result_code,
    }
    if workflow is not None:
        details["workflow_code"] = workflow.code
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
    project: ResolvedProject,
    selection: SelectedValue,
) -> SelectionData:
    return with_selection_source(cast("SelectionData", project.to_data()), selection)


def _resolved_workflow_selection(
    workflow: ResolvedWorkflow,
    selection: SelectedValue,
) -> SelectionData:
    return with_selection_source(cast("SelectionData", workflow.to_data()), selection)


def _resolved_task_selection(
    task: ResolvedTask,
    selection: SelectedValue,
) -> SelectionData:
    return with_selection_source(cast("SelectionData", task.to_data()), selection)


def _dependent_tasks_resolved_payload(
    *,
    target: _ResolvedWorkflowLineageTarget,
    selected_task: SelectedValue | None,
    resolved_task: ResolvedTask | None,
) -> dict[str, SelectionData]:
    resolved: dict[str, SelectionData] = {
        "project": _resolved_project_selection(
            target.resolved_project,
            target.selected_project,
        ),
        "workflow": _resolved_workflow_selection(
            target.resolved_workflow,
            target.selected_workflow,
        ),
    }
    if selected_task is not None and resolved_task is not None:
        resolved["task"] = _resolved_task_selection(resolved_task, selected_task)
    return resolved
