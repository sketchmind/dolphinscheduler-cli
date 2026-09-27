from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.cli_surface import WORKFLOW_INSTANCE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    InvalidStateError,
)
from dsctl.upstream.serialization import require_resource_int

if TYPE_CHECKING:
    from dsctl.support.yaml_io import JsonObject

INTERNAL_SERVER_ERROR_ARGS = 10000
MASTER_NOT_EXISTS = 10025


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
