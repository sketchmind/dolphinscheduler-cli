"""Language-neutral visibility rules for executable REST contract inputs."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ds_codegen.ir import OperationSpec, ParameterSpec


SERVER_INJECTED_PARAMETER_TYPES = frozenset(
    {
        "HttpServletRequest",
        "HttpServletResponse",
        "HttpSession",
        "ServletRequest",
        "ServletResponse",
        "User",
    }
)


def is_client_supplied_parameter(parameter: ParameterSpec) -> bool:
    """Return whether a REST caller can supply this operation parameter."""

    if parameter.binding == "request_attribute":
        return False
    if parameter.binding is not None:
        return True
    if parameter.hidden:
        return False
    return parameter.java_type.rsplit(".", 1)[-1] not in SERVER_INJECTED_PARAMETER_TYPES


def has_executable_response_type(operation: OperationSpec) -> bool:
    """Return whether the logical type describes the decoded REST response."""

    raw_base = operation.return_type.split("<", 1)[0]
    logical_base = operation.logical_return_type.split("<", 1)[0]
    raw_leaf = raw_base.rsplit(".", 1)[-1]
    logical_leaf = logical_base.rsplit(".", 1)[-1]
    return not (raw_leaf == "ResponseEntity" and logical_leaf in {"Resource", "byte[]"})


def is_required_parameter(parameter: ParameterSpec) -> bool:
    """Return the requiredness used by executable request model rendering."""
    return parameter.required is True or (
        parameter.required is None and parameter.default_value is None
    )


__all__ = [
    "SERVER_INJECTED_PARAMETER_TYPES",
    "has_executable_response_type",
    "is_client_supplied_parameter",
    "is_required_parameter",
]
