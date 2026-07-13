from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.cli_surface import WORKFLOW_INSTANCE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
)
from dsctl.services._serialization import require_resource_int

if TYPE_CHECKING:
    from dsctl.services.runtime import ServiceRuntime
    from dsctl.support.yaml_io import JsonObject
    from dsctl.upstream.protocol import WorkflowInstanceRecord

INTERNAL_SERVER_ERROR_ARGS = 10000
MASTER_NOT_EXISTS = 10025
WORKFLOW_INSTANCE_NOT_EXIST = 50001
USER_NO_OPERATION_PERM = 30001
USER_NO_OPERATION_PROJECT_PERM = 30002


def master_unavailable_error(
    error: ApiResultError,
    *,
    operation: str,
    details: JsonObject,
    suggestion: str,
) -> InvalidStateError | None:
    """Translate a DS missing-master runtime failure without retrying it."""
    if not _is_master_unavailable_error(error):
        return None
    return InvalidStateError(
        "DolphinScheduler has no available master server for this runtime action.",
        details={**details, "operation": operation},
        source=error.source,
        suggestion=suggestion,
    )


def _is_master_unavailable_error(error: ApiResultError) -> bool:
    if error.result_code == MASTER_NOT_EXISTS:
        return True
    if error.result_code != INTERNAL_SERVER_ERROR_ARGS:
        return False
    compacted_message = "".join(
        character
        for character in error.result_message.casefold()
        if character.isalnum()
    )
    return (
        "nomasterserveravailable" in compacted_message
        or "masterdoesnotexist" in compacted_message
    )


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
        message = f"Workflow instance id {workflow_instance_id} was not found"
        raise NotFoundError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
            },
            source=exc.source,
            suggestion=(
                "Run `dsctl workflow-instance list` to inspect available "
                "workflow instance ids."
            ),
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
