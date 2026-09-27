from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.cli_surface import (
    PROJECT_RESOURCE,
    TASK_RESOURCE,
    WORKFLOW_RESOURCE,
)
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    NotFoundError,
    PermissionDeniedError,
)
from dsctl.services._legacy_dependent_references import (
    resolve_legacy_read_dependent_refs,
)
from dsctl.services._legacy_workflow_references import (
    resolve_legacy_read_workflow_refs,
)
from dsctl.services._task_resource_refs import (
    mr_resource_ids_from_legacy_graph,
    resolve_read_task_resource_refs,
)
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyWorkflowGraph,
    LegacyWorkflowGraphError,
    decode_legacy_workflow_graph,
)
from dsctl.upstream.resolver import (
    raise_workflow_read_api_error,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.upstream.definition_models import (
        ProjectRef,
    )
    from dsctl.upstream.protocol import (
        WorkflowDagRecord,
    )
    from dsctl.upstream.workflows import (
        LegacyWorkflowDefinitionSnapshot,
    )

from dsctl.services.workflow._selection import (
    _identity_field,
    _legacy_native_id,
    _native_code,
    _ResolvedWorkflowTarget,
)
from dsctl.services.workflow._types import (
    _LEGACY_PROCESS_INSTANCE_NOT_EXIST,
    _USER_NO_OPERATION_PERMISSION,
    PROJECT_NOT_FOUND,
    WORKFLOW_DEFINITION_NOT_EXIST,
    WorkflowServiceRuntime,
)


def _load_workflow_dag(
    runtime: WorkflowServiceRuntime,
    *,
    target: _ResolvedWorkflowTarget,
    action: str,
) -> WorkflowDagRecord:
    """Load one DAG and keep stable read-error typing at the service seam."""
    try:
        return runtime.domain.workflows.dag(target.scope, action=action)
    except ApiResultError as error:
        raise_workflow_read_api_error(
            error,
            project_code=_native_code(target.project, label="project"),
            workflow_code=_native_code(target.workflow, label="workflow"),
        )


def _allocate_workflow_task_codes(
    runtime: WorkflowServiceRuntime,
    *,
    project: ProjectRef,
    count: int,
    action: str,
) -> Sequence[int]:
    """Allocate server task codes and attach stable mutation context."""
    project_code = _native_code(project, label="project")
    details = {
        "resource": PROJECT_RESOURCE,
        "project_code": project_code,
        "action": action,
        "task_code_count": count,
    }
    try:
        return runtime.domain.workflows.allocate_task_codes(project, count)
    except ApiResultError as error:
        message = "DolphinScheduler could not allocate task codes."
        raise ApiTransportError(
            message,
            details={
                **details,
                "result_code": error.result_code,
                "result_message": error.result_message,
            },
            source=error.source,
            suggestion="Retry the workflow mutation after checking server health.",
        ) from error
    except ApiTransportError as error:
        raise ApiTransportError(
            error.message,
            details={**error.details, **details},
            source=error.source,
            suggestion=(
                error.suggestion
                or "Verify DolphinScheduler API health and version, then retry."
            ),
        ) from error


def _load_legacy_workflow_graph(
    runtime: WorkflowServiceRuntime,
    *,
    target: _ResolvedWorkflowTarget,
    action: str,
    hydrate_workflow_refs: bool = False,
    hydrate_resource_refs: bool = False,
) -> tuple[LegacyWorkflowDefinitionSnapshot, DecodedLegacyWorkflowGraph]:
    """Read and validate the exact 1.3.9 three-document graph as one unit."""
    try:
        snapshot = runtime.domain.workflows.legacy_definition(
            target.scope,
            action=action,
        )
    except ApiResultError as error:
        details = {
            "resource": WORKFLOW_RESOURCE,
            "action": action,
            _identity_field(target.project, prefix="project_"): (
                target.project.native.value
            ),
            _identity_field(target.workflow, prefix="workflow_"): (
                target.workflow.native.value
            ),
            "result_code": error.result_code,
            "result_message": error.result_message,
        }
        legacy_missing_definition = (
            runtime.profile.ds_version == "1.3.9"
            and error.result_code == _LEGACY_PROCESS_INSTANCE_NOT_EXIST
        )
        if (
            error.result_code
            in {
                PROJECT_NOT_FOUND,
                WORKFLOW_DEFINITION_NOT_EXIST,
            }
            or legacy_missing_definition
        ):
            message = "The selected legacy workflow was not found."
            raise NotFoundError(
                message,
                details=details,
            ) from error
        if error.result_code == _USER_NO_OPERATION_PERMISSION:
            message = "Current user cannot read the selected legacy workflow."
            raise PermissionDeniedError(
                message,
                details=details,
            ) from error
        message = "DolphinScheduler could not read the selected legacy workflow."
        raise ApiTransportError(
            message,
            details=details,
            source=error.source,
        ) from error
    try:
        graph = decode_legacy_workflow_graph(
            snapshot.process_definition_json,
            snapshot.locations,
            snapshot.connects,
        )
        read_resource_refs = (
            resolve_read_task_resource_refs(
                runtime.domain.task_resource_resolver,
                mr_resource_ids_from_legacy_graph(graph),
            )
            if hydrate_resource_refs
            else None
        )
        if hydrate_workflow_refs:
            workflow_refs = resolve_legacy_read_workflow_refs(
                runtime.domain.workflows,
                project=target.project,
                graph=graph,
                containing_workflow_id=_legacy_native_id(
                    target.workflow,
                    label="workflow",
                ),
                action=action,
            )
            dependent_refs = resolve_legacy_read_dependent_refs(
                runtime.domain.workflows,
                graph=graph,
                action=action,
            )
            graph = decode_legacy_workflow_graph(
                snapshot.process_definition_json,
                snapshot.locations,
                snapshot.connects,
                workflow_refs=workflow_refs,
                dependent_refs=dependent_refs,
                resource_refs=read_resource_refs,
            )
        elif read_resource_refs is not None:
            graph = decode_legacy_workflow_graph(
                snapshot.process_definition_json,
                snapshot.locations,
                snapshot.connects,
                resource_refs=read_resource_refs,
            )
    except LegacyWorkflowGraphError as error:
        message = "The legacy workflow graph is internally inconsistent."
        raise ApiTransportError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "action": action,
                _identity_field(target.workflow, prefix="workflow_"): (
                    target.workflow.native.value
                ),
                "reason": str(error),
            },
            suggestion=(
                "Repair the workflow graph in DolphinScheduler before retrying "
                "this CLI operation."
            ),
        ) from error
    return snapshot, graph


def _legacy_workflow_task_name(
    graph: DecodedLegacyWorkflowGraph,
    *,
    target: _ResolvedWorkflowTarget,
    selector: str,
) -> str:
    """Resolve the public task selector without exposing native string ids."""
    if any(candidate.name == selector for candidate in graph.tasks):
        return selector
    message = f"Task '{selector}' does not exist in workflow '{target.workflow.name}'."
    raise NotFoundError(
        message,
        details={
            "resource": TASK_RESOURCE,
            "task": selector,
            _identity_field(target.workflow, prefix="workflow_"): (
                target.workflow.native.value
            ),
            "workflow": target.workflow.name,
            "available_tasks": [task.name for task in graph.tasks],
        },
        suggestion="Pass one of the task names returned by `dsctl workflow get`.",
    )
