from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import TYPE_CHECKING

from dsctl.models.procedure_call import ProcedureCall
from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _projection_error,
    _reject_field,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.task_parameter_projection.types import (
        ProjectionDirection,
        TaskParameterProjectionError,
    )


def _try_project_opaque_procedure(
    payload: JsonObject,
    *,
    task_type: str,
) -> JsonObject | None:
    """Keep runtime-derived procedure evidence in its exact native shape."""
    if task_type != "PROCEDURE" or "outProperty" not in payload:
        return None
    return deepcopy(payload)


def _encode_procedure(payload: JsonObject, *, version: str) -> JsonObject:
    """Project one canonical JDBC procedure call onto its exact execution epoch."""
    encoded = deepcopy(payload)
    procedure_surface = get_task_authoring_surface(version).procedure
    _reject_field(
        encoded,
        "outProperty",
        version=version,
        direction="encode",
        task_type="PROCEDURE",
        field="task_params.outProperty",
        reason="runtime-only-field",
    )
    if procedure_surface.method_syntax == "bare-name":
        _reject_field(
            encoded,
            "varPool",
            version=version,
            direction="encode",
            task_type="PROCEDURE",
            field="task_params.varPool",
            reason="field-absent-in-version",
        )
    method = encoded.get("method")
    if not isinstance(method, str):
        raise _procedure_projection_error(
            version=version,
            direction="encode",
            reason="missing-canonical-call",
            message="PROCEDURE requires canonical task_params.method text",
        )
    call = ProcedureCall.parse(method)
    if call is None or not call.uses_positional_placeholders:
        raise _procedure_projection_error(
            version=version,
            direction="encode",
            reason="invalid-canonical-call",
            message=(
                "PROCEDURE method must use canonical JDBC syntax such as "
                "{call schema.refresh_daily(?,?)}"
            ),
        )
    procedure_name = call.name
    placeholder_count = len(call.arguments)
    parameter_names = _procedure_parameter_names(
        encoded,
        version=version,
        direction="encode",
    )
    if placeholder_count != len(parameter_names):
        raise _procedure_projection_error(
            version=version,
            direction="encode",
            reason="placeholder-count-mismatch",
            message="PROCEDURE method placeholder count must match localParams",
        )
    if procedure_surface.method_syntax == "bare-name":
        encoded["method"] = procedure_name
    elif procedure_surface.method_syntax == "jdbc-positional":
        encoded["method"] = ProcedureCall.from_parts(
            procedure_name,
            ("?",) * placeholder_count,
        ).render()
    else:
        encoded["method"] = ProcedureCall.from_parts(
            procedure_name,
            tuple(f"${{{name}}}" for name in parameter_names),
        ).render()
    return encoded


def _decode_procedure(payload: JsonObject, *, version: str) -> JsonObject:
    """Restore one representable exact procedure call to canonical JDBC syntax."""
    decoded = deepcopy(payload)
    procedure_surface = get_task_authoring_surface(version).procedure
    _reject_field(
        decoded,
        "outProperty",
        version=version,
        direction="decode",
        task_type="PROCEDURE",
        field="task_params.outProperty",
        reason="runtime-only-field",
    )
    if procedure_surface.method_syntax == "bare-name":
        _reject_field(
            decoded,
            "varPool",
            version=version,
            direction="decode",
            task_type="PROCEDURE",
            field="task_params.varPool",
            reason="field-absent-in-version",
        )
    parameter_names = _procedure_parameter_names(
        decoded,
        version=version,
        direction="decode",
    )
    method = decoded.get("method")
    if not isinstance(method, str):
        raise _procedure_projection_error(
            version=version,
            direction="decode",
            reason="missing-native-call",
            message="PROCEDURE native task_params.method must be text",
        )
    if procedure_surface.method_syntax == "bare-name":
        procedure_name = method.strip()
        if not ProcedureCall.has_valid_name(procedure_name):
            raise _procedure_projection_error(
                version=version,
                direction="decode",
                reason="unrepresentable-native-call",
                message="PROCEDURE 1.3.9 method is not a qualified procedure name",
            )
    else:
        native_call = ProcedureCall.parse(method)
        if native_call is None:
            raise _procedure_projection_error(
                version=version,
                direction="decode",
                reason="unrepresentable-native-call",
                message="PROCEDURE native method is outside the canonical call subset",
            )
        procedure_name = native_call.name
        native_arguments = native_call.arguments
        if procedure_surface.method_syntax == "jdbc-positional":
            expected_arguments = ("?",) * len(parameter_names)
        else:
            expected_arguments = tuple(f"${{{name}}}" for name in parameter_names)
        normalized_arguments = tuple(
            _unquote_procedure_argument(argument) for argument in native_arguments
        )
        if normalized_arguments != expected_arguments:
            raise _procedure_projection_error(
                version=version,
                direction="decode",
                reason="native-parameter-order-mismatch",
                message=(
                    "PROCEDURE native method arguments do not match localParams "
                    "in source order"
                ),
            )
    decoded["method"] = ProcedureCall.from_parts(
        procedure_name,
        ("?",) * len(parameter_names),
    ).render()
    return decoded


def _procedure_parameter_names(
    payload: Mapping[str, JsonValue],
    *,
    version: str,
    direction: ProjectionDirection,
) -> tuple[str, ...]:
    raw_parameters = payload.get("localParams", [])
    if not isinstance(raw_parameters, Sequence) or isinstance(
        raw_parameters,
        (str, bytes, bytearray),
    ):
        raise _procedure_projection_error(
            version=version,
            direction=direction,
            reason="invalid-local-params",
            message="PROCEDURE task_params.localParams must be an array",
        )
    names: list[str] = []
    for index, parameter in enumerate(raw_parameters):
        if not isinstance(parameter, Mapping):
            raise _procedure_projection_error(
                version=version,
                direction=direction,
                reason="invalid-local-param",
                message=f"PROCEDURE localParams[{index}] must be an object",
            )
        name = parameter.get("prop")
        if not isinstance(name, str) or not name.strip():
            raise _procedure_projection_error(
                version=version,
                direction=direction,
                reason="invalid-local-param-name",
                message=f"PROCEDURE localParams[{index}].prop must be non-empty",
            )
        names.append(name.strip())
    if len(set(names)) != len(names):
        raise _procedure_projection_error(
            version=version,
            direction=direction,
            reason="duplicate-local-param-name",
            message="PROCEDURE localParams prop names must be unique",
        )
    return tuple(names)


def _unquote_procedure_argument(argument: str) -> str:
    if len(argument) >= 2 and argument[0] == argument[-1] and argument[0] in {"'", '"'}:
        return argument[1:-1]
    return argument


def _procedure_projection_error(
    *,
    version: str,
    direction: ProjectionDirection,
    reason: str,
    message: str,
) -> TaskParameterProjectionError:
    return _projection_error(
        version=version,
        direction=direction,
        task_type="PROCEDURE",
        field="task_params.method",
        reason=reason,
        message=message,
    )
