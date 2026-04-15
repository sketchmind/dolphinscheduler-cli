from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.cli_surface import WORKFLOW_INSTANCE_RESOURCE
from dsctl.errors import ApiResultError, NotFoundError
from dsctl.services._serialization import require_resource_int

if TYPE_CHECKING:
    from dsctl.services.runtime import ServiceRuntime
    from dsctl.upstream.protocol import WorkflowInstanceRecord


def get_workflow_instance(
    runtime: ServiceRuntime,
    *,
    workflow_instance_id: int,
) -> WorkflowInstanceRecord:
    """Fetch one workflow instance and normalize the missing-instance error."""
    try:
        return runtime.upstream.workflow_instances.get(
            workflow_instance_id=workflow_instance_id
        )
    except ApiResultError as exc:
        message = f"Workflow instance id {workflow_instance_id} was not found"
        raise NotFoundError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
            },
        ) from exc


def require_workflow_instance_project_code(
    value: int | None,
) -> int:
    """Require the owning project code from one workflow-instance payload."""
    return require_resource_int(
        value,
        resource=WORKFLOW_INSTANCE_RESOURCE,
        field_name="projectCode",
    )


def require_workflow_definition_code(
    value: int | None,
) -> int:
    """Require the workflow definition code from one workflow-instance payload."""
    return require_resource_int(
        value,
        resource=WORKFLOW_INSTANCE_RESOURCE,
        field_name="workflowDefinitionCode",
    )
