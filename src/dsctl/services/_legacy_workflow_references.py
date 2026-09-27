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
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyWorkflowGraph,
    LegacyWorkflowGraphError,
    LegacyWorkflowRefIndex,
    PreparedLegacyWorkflowGraph,
    prepared_legacy_runtime_nested_workflow_definition_ids,
)
from dsctl.upstream.legacy_workflow_references import (
    LegacyChildWorkflowResolutionError,
    LegacyDescendantWorkflowError,
    LegacyNestedWorkflowCycleError,
    LegacyNestedWorkflowLimitError,
    LegacyRecursiveWorkflowError,
    LegacyWorkflowAuthoringResolution,
    LegacyWorkflowReferenceOperations,
    LegacyWorkflowReferenceResolver,
)

if TYPE_CHECKING:
    from dsctl.models.workflow_spec import WorkflowSpec
    from dsctl.support.yaml_io import JsonObject
    from dsctl.upstream.definition_models import ProjectRef


_PERMISSION_RESULT_CODES = frozenset({30_001, 30_002})
_LEGACY_WORKFLOW_NOT_FOUND_CODES = frozenset({50_001, 50_003})


def resolve_legacy_authoring_workflow_refs(
    operations: LegacyWorkflowReferenceOperations,
    *,
    project: ProjectRef,
    spec: WorkflowSpec,
    containing_workflow_id: int | None,
    action: str,
) -> LegacyWorkflowAuthoringResolution:
    """Resolve exact 1.3.9 canonical child names with stable service errors."""
    resolver = LegacyWorkflowReferenceResolver(
        operations,
        project=project,
        action=action,
    )
    try:
        return resolver.resolve_authoring(
            spec,
            containing_workflow_id=containing_workflow_id,
        )
    except LegacyRecursiveWorkflowError as error:
        _raise_recursive_reference(error.selector, action=action)
    except LegacyChildWorkflowResolutionError as error:
        _raise_child_resolution_error(
            error,
            project=project,
            action=action,
        )
    except LegacyWorkflowGraphError as error:
        _raise_descendant_validation_error(
            action=action,
            project_name=project.name or "<unknown>",
            reason=str(error),
            cause=error,
        )


def audit_legacy_authoring_workflow_graph(
    operations: LegacyWorkflowReferenceOperations,
    *,
    project: ProjectRef,
    compilation: PreparedLegacyWorkflowGraph,
    containing_workflow_id: int | None,
    resolution: LegacyWorkflowAuthoringResolution,
    action: str,
) -> None:
    """Translate one exact runtime-equivalent descendant audit at the service seam."""
    resolver = LegacyWorkflowReferenceResolver(
        operations,
        project=project,
        action=action,
    )
    try:
        resolver.audit_compilation(
            compilation,
            containing_workflow_id=containing_workflow_id,
            resolution=resolution,
        )
    except LegacyNestedWorkflowCycleError as error:
        _raise_cycle(error.cycle, action=action)
    except LegacyNestedWorkflowLimitError as error:
        message = "Legacy nested-workflow validation exceeded its safety limit."
        raise UserInputError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "action": action,
                "limit": error.limit,
                "reason": "descendant-safety-limit",
            },
            suggestion="Reduce the nested-workflow closure and retry.",
        ) from error
    except LegacyDescendantWorkflowError as error:
        _raise_descendant_error(
            error,
            project=project,
            action=action,
        )


def requires_legacy_authoring_workflow_audit(
    compilation: PreparedLegacyWorkflowGraph,
    *,
    project: ProjectRef,
    action: str,
) -> bool:
    """Return whether a final graph has runtime edges, with stable errors."""
    try:
        return bool(prepared_legacy_runtime_nested_workflow_definition_ids(compilation))
    except LegacyWorkflowGraphError as error:
        _raise_descendant_validation_error(
            action=action,
            project_name=project.name or "<unknown>",
            reason=str(error),
            cause=error,
        )


def resolve_legacy_read_workflow_refs(
    operations: LegacyWorkflowReferenceOperations,
    *,
    project: ProjectRef,
    graph: DecodedLegacyWorkflowGraph,
    containing_workflow_id: int | None,
    action: str,
) -> LegacyWorkflowRefIndex:
    """Best-effort reverse-bind only immediately resolvable safe native packages."""
    return LegacyWorkflowReferenceResolver(
        operations,
        project=project,
        action=action,
    ).reverse_bind_immediate(
        graph,
        containing_workflow_id=containing_workflow_id,
    )


def _raise_child_resolution_error(
    error: LegacyChildWorkflowResolutionError,
    *,
    project: ProjectRef,
    action: str,
) -> NoReturn:
    selector = error.selector
    project_name = project.name or "<unknown>"
    cause = error.cause
    message = (
        f"Child workflow '{selector}' could not be resolved in project "
        f"'{project_name}'."
    )
    details: JsonObject = {
        "resource": WORKFLOW_RESOURCE,
        "action": action,
        "child_workflow": selector,
        "project": project_name,
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
            suggestion="Request read access to the selected child workflow.",
        ) from error
    if isinstance(cause, (NotFoundError, ResolutionError)) or (
        isinstance(cause, ApiResultError)
        and cause.result_code in _LEGACY_WORKFLOW_NOT_FOUND_CODES
    ):
        raise UserInputError(
            message,
            details=details,
            source=cause.source if isinstance(cause, DsctlError) else None,
            suggestion=(
                "Pass an exact workflow name returned by "
                "`dsctl workflow list --project PROJECT`."
            ),
        ) from error
    if isinstance(cause, ApiTransportError):
        raise cause from error
    if isinstance(cause, ApiResultError):
        transport_message = (
            "DolphinScheduler could not resolve the selected child workflow."
        )
        raise ApiTransportError(
            transport_message,
            details=details,
            source=cause.source,
        ) from error
    if isinstance(cause, DsctlError):
        raise cause from error
    _raise_descendant_validation_error(
        action=action,
        project_name=project_name,
        reason=str(cause),
        cause=error,
    )


def _raise_descendant_error(
    error: LegacyDescendantWorkflowError,
    *,
    project: ProjectRef,
    action: str,
) -> NoReturn:
    project_name = project.name or "<unknown>"
    cause = error.cause
    details: JsonObject = {
        "resource": WORKFLOW_RESOURCE,
        "action": action,
        "project": project_name,
        "reason": "nested-workflow-descendant-read",
    }
    if error.workflow_id is not None:
        details["workflow_id"] = error.workflow_id
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
        message = "Current user cannot validate a nested-workflow descendant."
        raise PermissionDeniedError(
            message,
            details=details,
            source=cause.source if isinstance(cause, DsctlError) else None,
            suggestion="Request read access to every nested workflow and retry.",
        ) from error
    if isinstance(
        cause,
        (LegacyWorkflowGraphError, NotFoundError, ResolutionError),
    ) or (
        isinstance(cause, ApiResultError)
        and cause.result_code in _LEGACY_WORKFLOW_NOT_FOUND_CODES
    ):
        _raise_descendant_validation_error(
            action=action,
            project_name=project_name,
            reason=getattr(cause, "message", str(cause)),
            cause=error,
        )
    if isinstance(cause, ApiTransportError):
        raise cause from error
    if isinstance(cause, ApiResultError):
        message = "DolphinScheduler could not validate nested-workflow descendants."
        raise ApiTransportError(
            message,
            details=details,
            source=cause.source,
        ) from error
    if isinstance(cause, DsctlError):
        raise cause from error
    _raise_descendant_validation_error(
        action=action,
        project_name=project_name,
        reason=str(cause),
        cause=error,
    )


def _raise_descendant_validation_error(
    *,
    action: str,
    project_name: str,
    reason: str,
    cause: BaseException,
) -> NoReturn:
    message = "Legacy nested-workflow descendants could not be safely validated."
    raise UserInputError(
        message,
        details={
            "resource": WORKFLOW_RESOURCE,
            "action": action,
            "project": project_name,
            "reason": reason,
        },
        suggestion=(
            "Repair every same-project child workflow reference and cycle, then retry."
        ),
    ) from cause


def _raise_recursive_reference(selector: str, *, action: str) -> NoReturn:
    message = f"Child workflow '{selector}' would recursively reference its parent."
    raise UserInputError(
        message,
        details={
            "resource": WORKFLOW_RESOURCE,
            "action": action,
            "child_workflow": selector,
            "reason": "direct-self-reference",
        },
        suggestion="Select a non-recursive child workflow in the same project.",
    )


def _raise_cycle(cycle: tuple[int, ...], *, action: str) -> NoReturn:
    rendered = " -> ".join(str(workflow_id) for workflow_id in cycle)
    message = f"Legacy nested-workflow cycle detected: {rendered}."
    raise UserInputError(
        message,
        details={
            "resource": WORKFLOW_RESOURCE,
            "action": action,
            "workflow_ids": list(cycle),
            "reason": "nested-workflow-cycle",
        },
        suggestion="Remove the recursive processDefinitionId reference and retry.",
    )


__all__ = [
    "audit_legacy_authoring_workflow_graph",
    "requires_legacy_authoring_workflow_audit",
    "resolve_legacy_authoring_workflow_refs",
    "resolve_legacy_read_workflow_refs",
]
