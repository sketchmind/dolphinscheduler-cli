from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from dsctl.errors import ApiHttpError, ApiResultError, ApiTransportError, DsctlError
from dsctl.upstream.wire import WireContractError

if TYPE_CHECKING:
    from collections.abc import Callable

    from dsctl.upstream._generated_types import OpaqueGeneratedValue

RecordT = TypeVar("RecordT")


def mutation_call(
    call: Callable[[], OpaqueGeneratedValue],
    *,
    ds_version: str,
    resource: str,
    operation: str,
) -> OpaqueGeneratedValue:
    """Dispatch one non-retryable mutation and classify uncertain outcomes."""
    try:
        return call()
    except ApiTransportError as exc:
        if _is_response_error(exc):
            raise mutation_verification_error(
                exc,
                ds_version=ds_version,
                resource=resource,
                operation=operation,
                phase="mutation_response",
            ) from exc
        raise mutation_dispatch_error(
            exc,
            ds_version=ds_version,
            resource=resource,
            operation=operation,
        ) from exc
    except ApiHttpError as exc:
        raise mutation_dispatch_error(
            exc,
            ds_version=ds_version,
            resource=resource,
            operation=operation,
        ) from exc
    except ApiResultError as exc:
        if exc.result_code is not None:
            raise
        raise mutation_dispatch_error(
            exc,
            ds_version=ds_version,
            resource=resource,
            operation=operation,
        ) from exc


def verify_mutation(
    verification: Callable[[], RecordT],
    *,
    ds_version: str,
    resource: str,
    operation: str,
    phase: str = "readback",
) -> RecordT:
    """Translate post-dispatch projection/readback failures consistently."""
    try:
        return verification()
    except (DsctlError, TypeError, WireContractError) as exc:
        raise mutation_verification_error(
            exc,
            ds_version=ds_version,
            resource=resource,
            operation=operation,
            phase=phase,
        ) from exc


def mutation_dispatch_error(
    cause: DsctlError,
    *,
    ds_version: str,
    resource: str,
    operation: str,
) -> ApiTransportError:
    """Build the stable error for an uncertain post-dispatch mutation."""
    details = dict(cause.details)
    details.update(
        {
            "ds_version": ds_version,
            "resource": resource,
            "operation": operation,
            "phase": "mutation_request",
            "mutation_may_have_applied": True,
        }
    )
    return ApiTransportError(
        f"{_resource_label(resource)} {operation} transport failed after dispatch; "
        "it may have applied",
        details=details,
        source=getattr(cause, "source", None),
        suggestion=reconciliation_suggestion(resource, operation),
    )


def mutation_verification_error(
    cause: BaseException,
    *,
    ds_version: str,
    resource: str,
    operation: str,
    phase: str,
) -> ApiTransportError:
    """Build the stable error for a mutation whose result cannot be verified."""
    details = dict(cause.details) if isinstance(cause, DsctlError) else {}
    details.update(
        {
            "ds_version": ds_version,
            "resource": resource,
            "operation": operation,
            "phase": phase,
            "mutation_applied": True,
        }
    )
    suggestion = reconciliation_suggestion(resource, operation)
    if isinstance(cause, DsctlError) and cause.suggestion is not None:
        suggestion = f"{suggestion} {cause.suggestion}"
    return ApiTransportError(
        f"{_resource_label(resource)} {operation} succeeded, but its response "
        "could not be verified",
        details=details,
        source={
            "kind": "remote",
            "system": "dolphinscheduler",
            "layer": "response",
        },
        suggestion=suggestion,
    )


def reconciliation_suggestion(resource: str, operation: str) -> str:
    """Return the shared mutation reconciliation guidance."""
    return (
        f"Inspect the {resource} before deciding whether to retry {resource} "
        f"{operation}; do not blindly repeat the mutation."
    )


def _is_response_error(error: ApiTransportError) -> bool:
    source = error.source
    return source is not None and source.get("layer") == "response"


def _resource_label(resource: str) -> str:
    return resource.replace("-", " ").capitalize()


__all__ = [
    "mutation_call",
    "mutation_dispatch_error",
    "mutation_verification_error",
    "reconciliation_suggestion",
    "verify_mutation",
]
