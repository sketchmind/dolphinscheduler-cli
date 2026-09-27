from __future__ import annotations

from typing import TYPE_CHECKING, NoReturn

from dsctl.cli_surface import WORKFLOW_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    DsctlError,
    NotFoundError,
    PermissionDeniedError,
    ResolutionError,
    UserInputError,
)
from dsctl.upstream.legacy_dependent_references import (
    LegacyDependentReferenceOperations,
    LegacyDependentReferenceResolver,
    LegacyDependentTargetResolutionError,
)
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyWorkflowGraph,
    LegacyDependentRefIndex,
    LegacyWorkflowGraphError,
)

if TYPE_CHECKING:
    from dsctl.models.workflow_spec import WorkflowSpec
    from dsctl.support.yaml_io import JsonObject


_PERMISSION_RESULT_CODES = frozenset({30_001, 30_002})
_NOT_FOUND_RESULT_CODES = frozenset({10_018, 10_190, 50_001, 50_003})


def resolve_legacy_authoring_dependent_refs(
    operations: LegacyDependentReferenceOperations,
    *,
    spec: WorkflowSpec,
    action: str,
) -> LegacyDependentRefIndex:
    """Resolve exact 1.3.9 DEPENDENT names with stable service errors."""
    try:
        return LegacyDependentReferenceResolver(
            operations,
            action=action,
        ).resolve_authoring(spec)
    except LegacyDependentTargetResolutionError as error:
        _raise_target_resolution_error(error, action=action)


def resolve_legacy_read_dependent_refs(
    operations: LegacyDependentReferenceOperations,
    *,
    graph: DecodedLegacyWorkflowGraph,
    action: str,
) -> LegacyDependentRefIndex:
    """Best-effort reverse-bind exact safe native DEPENDENT packages."""
    return LegacyDependentReferenceResolver(
        operations,
        action=action,
    ).reverse_bind(graph)


def _raise_target_resolution_error(
    error: LegacyDependentTargetResolutionError,
    *,
    action: str,
) -> NoReturn:
    cause = error.cause
    target = (
        f"project '{error.project_name}', workflow '{error.workflow_name}'"
        if error.task_name is None
        else (
            f"project '{error.project_name}', workflow '{error.workflow_name}', "
            f"task '{error.task_name}'"
        )
    )
    message = f"DEPENDENT target {target} could not be resolved."
    details: JsonObject = {
        "resource": WORKFLOW_RESOURCE,
        "action": action,
        "dependent_project": error.project_name,
        "dependent_workflow": error.workflow_name,
        "dependent_task": error.task_name,
    }
    if isinstance(cause, DsctlError):
        details.update(cause.details)
    if isinstance(cause, ApiResultError):
        details.update(
            {
                "result_code": cause.result_code,
                "result_message": cause.result_message,
            }
        )

    if isinstance(cause, PermissionDeniedError) or (
        isinstance(cause, ApiResultError)
        and cause.result_code in _PERMISSION_RESULT_CODES
    ):
        raise PermissionDeniedError(
            message,
            details=details,
            source=cause.source if isinstance(cause, DsctlError) else None,
            suggestion="Request read access to the selected DEPENDENT target.",
        ) from error

    if isinstance(cause, (NotFoundError, ResolutionError)) or (
        isinstance(cause, ApiResultError)
        and cause.result_code in _NOT_FOUND_RESULT_CODES
    ):
        raise UserInputError(
            message,
            details=details,
            source=cause.source if isinstance(cause, DsctlError) else None,
            suggestion=(
                "Use exact project, workflow, and task names returned by the "
                "corresponding dsctl list/get commands."
            ),
        ) from error

    if isinstance(cause, LegacyWorkflowGraphError):
        raise UserInputError(
            message,
            details={**details, "reason": str(cause)},
            suggestion=(
                "Use an existing literal target name and retry the workflow mutation."
            ),
        ) from error

    if isinstance(cause, ApiTransportError):
        raise ApiTransportError(
            cause.message,
            details={**cause.details, **details},
            source=cause.source,
            suggestion=cause.suggestion,
        ) from error

    if isinstance(cause, ApiResultError):
        transport_message = (
            "DolphinScheduler could not resolve the selected DEPENDENT target."
        )
        raise ApiTransportError(
            transport_message,
            details=details,
            source=cause.source,
            suggestion="Check DolphinScheduler API health and retry.",
        ) from error

    if isinstance(cause, DsctlError):
        raise cause from error
    transport_message = "The DEPENDENT target could not be resolved safely."
    raise ApiTransportError(
        transport_message,
        details={**details, "reason": str(cause)},
        suggestion="Check the target identity and retry.",
    ) from error


__all__ = [
    "resolve_legacy_authoring_dependent_refs",
    "resolve_legacy_read_dependent_refs",
]
