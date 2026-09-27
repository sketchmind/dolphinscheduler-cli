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
from dsctl.upstream.dynamic_workflow_references import (
    MAX_DYNAMIC_DESCENDANT_WORKFLOWS,
    DynamicChildWorkflowResolutionError,
    DynamicWorkflowCycleError,
    DynamicWorkflowLimitError,
    DynamicWorkflowReadError,
    DynamicWorkflowReferenceAuditor,
    DynamicWorkflowReferenceOperations,
    DynamicWorkflowReferenceResolver,
    DynamicWorkflowShapeError,
)
from dsctl.upstream.task_parameter_projection import TaskWorkflowRefIndex

if TYPE_CHECKING:
    from dsctl.upstream.definition_models import ProjectRef
    from dsctl.upstream.serialization import StructuredDataValue

_PERMISSION_RESULT_CODES = frozenset({30_001, 30_002})
_WORKFLOW_NOT_FOUND_CODES = frozenset({50_001, 50_003})


def resolve_dynamic_authoring_workflow_refs(
    operations: DynamicWorkflowReferenceOperations | None,
    *,
    project: ProjectRef,
    child_workflow_names: tuple[str, ...],
    containing_workflow_code: int | None,
    audit_descendants: bool,
    action: str,
    boundary_resource: str = WORKFLOW_RESOURCE,
) -> TaskWorkflowRefIndex:
    """Resolve child names, then audit their closure for changes, including previews."""
    if not child_workflow_names:
        return TaskWorkflowRefIndex.from_code_by_name({})
    if operations is None:
        _raise_missing_workflow_reads(
            child_workflow_names=child_workflow_names,
            action=action,
            boundary_resource=boundary_resource,
        )
    try:
        workflow_refs = DynamicWorkflowReferenceResolver(
            operations,
            project=project,
        ).resolve_authoring(child_workflow_names)
    except DynamicChildWorkflowResolutionError as error:
        _raise_child_resolution_error(
            error,
            project=project,
            action=action,
            boundary_resource=boundary_resource,
        )
    if audit_descendants:
        _audit_dynamic_workflow_references(
            operations,
            project=project,
            roots=tuple(
                workflow_refs.code_by_name[name] for name in child_workflow_names
            ),
            containing_workflow_code=containing_workflow_code,
            action=action,
            boundary_resource=boundary_resource,
        )
    return workflow_refs


def resolve_read_dynamic_workflow_refs(
    operations: DynamicWorkflowReferenceOperations | None,
    *,
    project: ProjectRef,
    workflow_codes: tuple[int, ...],
) -> TaskWorkflowRefIndex:
    """Best-effort reverse-bind visible child codes; unresolved wire stays opaque."""
    if operations is None or not workflow_codes:
        return TaskWorkflowRefIndex.from_code_by_name({})
    return DynamicWorkflowReferenceResolver(
        operations,
        project=project,
    ).reverse_bind(workflow_codes)


def _audit_dynamic_workflow_references(
    operations: DynamicWorkflowReferenceOperations,
    *,
    project: ProjectRef,
    roots: tuple[int, ...],
    containing_workflow_code: int | None,
    action: str,
    boundary_resource: str,
) -> None:
    """Translate the upstream exact native closure audit at the owning boundary."""
    try:
        DynamicWorkflowReferenceAuditor(
            operations,
            project=project,
            action=action,
        ).audit(
            roots,
            containing_workflow_code=_optional_positive_code(
                containing_workflow_code,
                label="containing workflow",
            ),
        )
    except DynamicWorkflowCycleError as error:
        rendered = " -> ".join(str(code) for code in error.cycle)
        message = f"DYNAMIC child-workflow cycle detected: {rendered}."
        raise UserInputError(
            message,
            details={
                "workflow_codes": list(error.cycle),
                "resource": boundary_resource,
                "action": action,
                "reason": "dynamic-child-workflow-cycle",
            },
            suggestion=(
                "Remove the recursive DYNAMIC or SUB_PROCESS reference and retry."
            ),
        ) from error
    except DynamicWorkflowLimitError as error:
        message = "DYNAMIC child-workflow validation exceeded its safety limit."
        raise UserInputError(
            message,
            details={
                "limit": error.limit,
                "resource": boundary_resource,
                "action": action,
                "reason": "dynamic-descendant-safety-limit",
            },
            suggestion="Reduce the nested-workflow closure and retry.",
        ) from error
    except DynamicWorkflowReadError as error:
        _raise_dynamic_workflow_read_error(
            error,
            project=project,
            action=action,
            boundary_resource=boundary_resource,
        )
    except DynamicWorkflowShapeError as error:
        message = "A DYNAMIC child workflow has an unsafe nested-workflow reference."
        raise UserInputError(
            message,
            details={
                "workflow_code": error.workflow_code,
                "native_reason": error.reason,
                "resource": boundary_resource,
                "action": action,
                "reason": "dynamic-child-workflow-shape",
            },
            suggestion=(
                "Repair every DYNAMIC and SUB_PROCESS process-definition code "
                "in the child closure, then retry."
            ),
        ) from error


def _raise_child_resolution_error(
    error: DynamicChildWorkflowResolutionError,
    *,
    project: ProjectRef,
    action: str,
    boundary_resource: str,
) -> NoReturn:
    cause = error.cause
    project_name = project.name or str(project.native.value)
    details = _boundary_error_details(
        cause,
        boundary_resource=boundary_resource,
        action=action,
        reason="dynamic-child-workflow-resolution",
        project=project_name,
        child_workflow_name=error.selector,
    )
    if _is_permission_error(cause):
        message = "Current user cannot resolve a DYNAMIC child workflow."
        raise PermissionDeniedError(
            message,
            details=details,
            source=cause.source if isinstance(cause, DsctlError) else None,
            suggestion="Request read access to the child workflow and retry.",
        ) from error
    if _is_not_found_error(cause) or isinstance(cause, (TypeError, ValueError)):
        message = (
            f"DYNAMIC child workflow {error.selector!r} was not found in "
            f"project {project_name!r}."
        )
        raise UserInputError(
            message,
            details=details,
            source=cause.source if isinstance(cause, DsctlError) else None,
            suggestion=(
                "Choose a name returned by `dsctl workflow list --project PROJECT` "
                "and retry."
            ),
        ) from error
    if isinstance(cause, ApiResultError):
        message = "DolphinScheduler could not resolve a DYNAMIC child workflow."
        raise ApiTransportError(
            message,
            details=details,
            source=cause.source,
        ) from error
    if isinstance(cause, ApiTransportError):
        raise ApiTransportError(
            cause.message,
            details=details,
            source=cause.source,
            suggestion=cause.suggestion,
        ) from error
    message = "DYNAMIC child-workflow resolution failed."
    raise ApiTransportError(
        message,
        details=details,
        source=cause.source if isinstance(cause, DsctlError) else None,
    ) from error


def _raise_dynamic_workflow_read_error(
    error: DynamicWorkflowReadError,
    *,
    project: ProjectRef,
    action: str,
    boundary_resource: str,
) -> NoReturn:
    cause = error.cause
    project_name = project.name or str(project.native.value)
    details = _boundary_error_details(
        cause,
        boundary_resource=boundary_resource,
        action=action,
        reason="dynamic-child-workflow-read",
        project=project_name,
        workflow_code=error.workflow_code,
    )
    if _is_permission_error(cause):
        message = "Current user cannot validate a DYNAMIC child workflow."
        raise PermissionDeniedError(
            message,
            details=details,
            source=cause.source if isinstance(cause, DsctlError) else None,
            suggestion="Request read access to every child workflow and retry.",
        ) from error
    if _is_not_found_error(cause):
        message = (
            f"DYNAMIC child workflow code {error.workflow_code} was not found "
            f"in project {project_name!r}."
        )
        raise UserInputError(
            message,
            details=details,
            source=cause.source if isinstance(cause, DsctlError) else None,
            suggestion=(
                "Re-export the parent workflow, verify every child still exists, "
                "and retry."
            ),
        ) from error
    if isinstance(cause, ApiResultError):
        message = "DolphinScheduler could not validate a DYNAMIC child workflow."
        raise ApiTransportError(
            message,
            details=details,
            source=cause.source,
        ) from error
    if isinstance(cause, ApiTransportError):
        raise ApiTransportError(
            cause.message,
            details=details,
            source=cause.source,
            suggestion=cause.suggestion,
        ) from error
    message = "DYNAMIC child-workflow preflight could not read a descendant."
    raise ApiTransportError(
        message,
        details=details,
        source=cause.source if isinstance(cause, DsctlError) else None,
    ) from error


def _boundary_error_details(
    cause: BaseException,
    *,
    boundary_resource: str,
    action: str,
    reason: str,
    **context: StructuredDataValue,
) -> dict[str, StructuredDataValue]:
    """Merge native evidence first, then make the owning service boundary final."""
    details: dict[str, StructuredDataValue] = {}
    if isinstance(cause, DsctlError):
        details.update(cause.details)
    if isinstance(cause, ApiResultError):
        details.update(
            {
                "result_code": cause.result_code,
                "result_message": cause.result_message,
            }
        )
    details.update(context)
    details.update({"resource": boundary_resource, "action": action, "reason": reason})
    return details


def _is_permission_error(cause: BaseException) -> bool:
    return isinstance(cause, PermissionDeniedError) or (
        isinstance(cause, ApiResultError)
        and cause.result_code in _PERMISSION_RESULT_CODES
    )


def _is_not_found_error(cause: BaseException) -> bool:
    return isinstance(cause, (NotFoundError, ResolutionError)) or (
        isinstance(cause, ApiResultError)
        and cause.result_code in _WORKFLOW_NOT_FOUND_CODES
    )


def _optional_positive_code(value: int | None, *, label: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        message = f"DYNAMIC {label} must use a positive native code"
        raise ValueError(message)
    return value


def _raise_missing_workflow_reads(
    *,
    child_workflow_names: tuple[str, ...],
    action: str,
    boundary_resource: str,
) -> NoReturn:
    message = "The selected runtime has no workflow-definition reads for DYNAMIC"
    raise ApiTransportError(
        message,
        details={
            "child_workflow_names": list(child_workflow_names),
            "resource": boundary_resource,
            "action": action,
            "reason": "dynamic-workflow-reads-unavailable",
        },
    )


__all__ = [
    "MAX_DYNAMIC_DESCENDANT_WORKFLOWS",
    "resolve_dynamic_authoring_workflow_refs",
    "resolve_read_dynamic_workflow_refs",
]
