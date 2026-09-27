"""Resolve route arguments without inventing facts in the native source IR."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace

from ds_codegen.contract_visibility import is_client_supplied_parameter
from ds_codegen.ir import OperationSpec, ParameterSpec


@dataclass(frozen=True)
class OperationPathArgument:
    """A route slot and its declared parameter, absent for a renderer-only arg."""

    name: str
    parameter: ParameterSpec | None


def operation_path_arguments(
    operation: OperationSpec,
) -> tuple[OperationPathArgument, ...]:
    """Keep the original wrapper's route order and hidden-parameter rejection."""
    parameters = {
        parameter.wire_name or parameter.name: parameter
        for parameter in operation.parameters
        if parameter.binding == "path_variable"
    }
    arguments: list[OperationPathArgument] = []
    for name in re.findall(r"\{([^{}]+)\}", operation.path):
        parameter = parameters.get(name)
        if parameter is not None and not is_client_supplied_parameter(parameter):
            message = (
                f"path placeholder {name!r} for {operation.operation_id} "
                "is not a client-supplied parameter"
            )
            raise ValueError(message)
        arguments.append(OperationPathArgument(name, parameter))
    return tuple(arguments)


def executable_path_operation(operation: OperationSpec) -> OperationSpec:
    """Materialize the old renderer's implicit integer args in a temporary view.

    The returned operation is executable-schema input only. Native source records,
    policies and snapshot digests must continue using the original operation.
    """
    derived = [
        ParameterSpec(
            name=argument.name,
            java_type="int",
            binding="path_variable",
            wire_name=argument.name,
            required=True,
            default_value=None,
            hidden=False,
            description=None,
            example=None,
            allowable_values=None,
            schema_type=None,
        )
        for argument in operation_path_arguments(operation)
        if argument.parameter is None
    ]
    return (
        replace(operation, parameters=[*derived, *operation.parameters])
        if derived
        else operation
    )
