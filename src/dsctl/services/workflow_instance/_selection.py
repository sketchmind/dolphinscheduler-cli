from __future__ import annotations

import shlex
from typing import TYPE_CHECKING

from dsctl.cli_surface import WORKFLOW_INSTANCE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    NotFoundError,
    PermissionDeniedError,
)
from dsctl.services.selection import (
    SelectedValue,
    require_project_selection,
)
from dsctl.upstream.definition_models import (
    NativeCode,
    ProjectRef,
)
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyWorkflowGraph,
    LegacyWorkflowGraphError,
    decode_legacy_workflow_graph,
)
from dsctl.upstream.resolver import (
    ResolvedProject,
)
from dsctl.upstream.runtime_enums import (
    WorkflowExecutionStatusInfo,
    workflow_execution_status_info,
)
from dsctl.upstream.runtime_instances import (
    LocatedWorkflowInstance,
    WorkflowInstanceSnapshot,
)
from dsctl.upstream.serialization import (
    optional_text,
)

if TYPE_CHECKING:
    from dsctl.support.yaml_io import JsonObject
    from dsctl.upstream.protocol import (
        StringEnumValue,
    )
    from dsctl.upstream.task_parameter_projection import (
        TaskResourceRefIndex,
    )


from dsctl.services.workflow_instance._types import (
    USER_NO_OPERATION_PERM,
    USER_NO_OPERATION_PROJECT_PERM,
    WORKFLOW_INSTANCE_NOT_EXIST,
    RuntimeInstanceServiceRuntime,
)


def get_workflow_instance(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    project_selector: str,
    workflow_instance_id: int,
) -> WorkflowInstanceSnapshot:
    """Fetch one instance through the exact domain and own stable errors here."""
    try:
        return runtime.domain.instances.get_workflow_instance(
            project_selector=project_selector,
            workflow_instance_id=workflow_instance_id,
        ).instance
    except ApiResultError as exc:
        if exc.result_code in {
            USER_NO_OPERATION_PERM,
            USER_NO_OPERATION_PROJECT_PERM,
        }:
            message = (
                "The current user does not have permission to access workflow "
                f"instance id {workflow_instance_id}"
            )
            raise PermissionDeniedError(
                message,
                details={
                    "resource": WORKFLOW_INSTANCE_RESOURCE,
                    "id": workflow_instance_id,
                },
                suggestion=(
                    "Ask a DolphinScheduler administrator to grant access to the "
                    "workflow instance's project, then retry."
                ),
            ) from exc
        if exc.result_code != WORKFLOW_INSTANCE_NOT_EXIST:
            raise
        list_command = shlex.join(
            ("dsctl", "workflow-instance", "list", "--project", project_selector)
        )
        message = f"Workflow instance id {workflow_instance_id} was not found"
        raise NotFoundError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
            },
            source=exc.source,
            suggestion=(
                f"Run `{list_command}` to inspect available workflow instance ids."
            ),
        ) from exc


def _located_workflow_instance(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    project_selector: str,
    workflow_instance_id: int,
) -> LocatedWorkflowInstance:
    payload = get_workflow_instance(
        runtime,
        project_selector=project_selector,
        workflow_instance_id=workflow_instance_id,
    )
    return LocatedWorkflowInstance(payload.project, payload)


def _selected_workflow_instance(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    project: str | None,
    workflow_instance_id: int,
) -> tuple[SelectedValue, LocatedWorkflowInstance]:
    """Resolve project context before sending one direct instance detail request."""
    selected_project = require_project_selection(project, runtime=runtime)
    return selected_project, _located_workflow_instance(
        runtime,
        project_selector=selected_project.value,
        workflow_instance_id=workflow_instance_id,
    )


def _resolved_project(project: ProjectRef) -> ResolvedProject:
    if not isinstance(project.native, NativeCode) or project.name is None:
        message = "Workflow-instance authoring requires code-native project metadata"
        raise ApiTransportError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "project": project.to_data(),
            },
        )
    return ResolvedProject(
        code=project.native.value,
        name=project.name,
        description=project.description,
    )


def _is_legacy_workflow_instance(payload: WorkflowInstanceSnapshot) -> bool:
    return payload.ds_version == "1.3.9"


def _legacy_workflow_instance_graph(
    payload: WorkflowInstanceSnapshot,
    *,
    resource_refs: TaskResourceRefIndex | None = None,
) -> DecodedLegacyWorkflowGraph:
    native_fields = {
        "processInstanceJson": payload.processInstanceJson,
        "locations": payload.locations,
        "connects": payload.connects,
    }
    missing = [name for name, value in native_fields.items() if value is None]
    if missing:
        message = "Legacy workflow instance payload was missing native graph fields"
        raise ApiTransportError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": payload.id,
                "ds_version": payload.ds_version,
                "missing_fields": missing,
            },
        )
    process_instance_json = payload.processInstanceJson
    locations = payload.locations
    connects = payload.connects
    if process_instance_json is None or locations is None or connects is None:
        message = "Legacy workflow instance graph projection was incomplete"
        raise RuntimeError(message)
    try:
        return decode_legacy_workflow_graph(
            process_instance_json,
            locations,
            connects,
            resource_refs=resource_refs,
        )
    except LegacyWorkflowGraphError as exc:
        message = "Legacy workflow instance graph was invalid"
        raise ApiTransportError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": payload.id,
                "ds_version": payload.ds_version,
                "reason": str(exc),
            },
        ) from exc


def _legacy_workflow_instance_name(payload: WorkflowInstanceSnapshot) -> str:
    name = optional_text(payload.name)
    if name is not None:
        return name
    message = "Legacy workflow instance payload was missing its workflow name"
    raise ApiTransportError(
        message,
        details={
            "resource": WORKFLOW_INSTANCE_RESOURCE,
            "id": payload.id,
            "ds_version": payload.ds_version,
        },
    )


def _legacy_workflow_instance_project_name(
    payload: WorkflowInstanceSnapshot,
) -> str:
    name = optional_text(payload.project.name)
    if name is not None:
        return name
    message = "Legacy workflow instance payload was missing its project name"
    raise ApiTransportError(
        message,
        details={
            "resource": WORKFLOW_INSTANCE_RESOURCE,
            "id": payload.id,
            "ds_version": payload.ds_version,
        },
    )


def _require_instance_workflow_code(payload: WorkflowInstanceSnapshot) -> int:
    code = payload.workflowDefinitionCode
    if code is not None:
        return code
    message = "Workflow instance payload was missing workflowDefinitionCode"
    raise ApiTransportError(
        message,
        details={
            "resource": WORKFLOW_INSTANCE_RESOURCE,
            "id": payload.id,
            "ds_version": payload.ds_version,
        },
    )


def _workflow_instance_resolved(
    workflow_instance_id: int,
    *,
    project: ProjectRef,
    selected_project: SelectedValue,
) -> JsonObject:
    workflow_instance: JsonObject = {"id": workflow_instance_id}
    return {
        "workflowInstance": workflow_instance,
        "project": _selected_project_data(project, selected_project),
    }


def _selected_project_data(
    project: ProjectRef,
    selected_project: SelectedValue,
) -> JsonObject:
    data = project.to_data()
    data["source"] = selected_project.source
    return data


def _workflow_execution_status(
    value: StringEnumValue | str | None,
) -> WorkflowExecutionStatusInfo | None:
    return workflow_execution_status_info(value)
