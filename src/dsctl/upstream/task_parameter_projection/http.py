from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

from pydantic import ValidationError

from dsctl.models.task_spec import (
    HttpTaskParamsSpec,
)
from dsctl.upstream.parameter_semantics import get_parameter_semantics
from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _projection_error,
    _reject_field,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream.task_parameter_projection.types import (
        ProjectionDirection,
    )

_HTTP_139_CANONICAL_FIELDS = frozenset(
    {
        "condition",
        "connectTimeout",
        "httpCheckCondition",
        "httpMethod",
        "httpParams",
        "localParams",
        "url",
    }
)


def _encode_http(payload: JsonObject, *, version: str) -> JsonObject:
    encoded = deepcopy(payload)
    if version == "1.3.9":
        _validate_http_139_canonical(
            encoded,
            version=version,
            direction="encode",
        )
    surface = get_task_authoring_surface(version).http
    if surface.native_socket_timeout is not None:
        if not surface.request_body:
            _reject_field(
                encoded,
                "httpBody",
                version=version,
                direction="encode",
                task_type="HTTP",
                field="task_params.httpBody",
                reason="field-absent-in-version",
            )
        if "socketTimeout" in encoded:
            message = "socketTimeout is native-only and cannot be explicitly authored"
            raise _projection_error(
                version=version,
                direction="encode",
                task_type="HTTP",
                field="task_params.socketTimeout",
                reason="native-only-field",
                message=message,
            )
        encoded["socketTimeout"] = surface.native_socket_timeout
        return encoded
    _reject_field(
        encoded,
        "socketTimeout",
        version=version,
        direction="encode",
        task_type="HTTP",
        field="task_params.socketTimeout",
        reason="field-absent-in-version",
    )
    return encoded


def _decode_http(payload: JsonObject, *, version: str) -> JsonObject:
    decoded = deepcopy(payload)
    surface = get_task_authoring_surface(version).http
    if surface.native_socket_timeout is not None:
        if not surface.request_body:
            _reject_field(
                decoded,
                "httpBody",
                version=version,
                direction="decode",
                task_type="HTTP",
                field="task_params.httpBody",
                reason="field-absent-in-version",
            )
        socket_timeout = decoded.get("socketTimeout")
        if socket_timeout != surface.native_socket_timeout:
            message = "A non-default native socketTimeout has no canonical typed field"
            raise _projection_error(
                version=version,
                direction="decode",
                task_type="HTTP",
                field="task_params.socketTimeout",
                reason="unrepresentable-native-value",
                message=message,
            )
        del decoded["socketTimeout"]
        if version == "1.3.9":
            _validate_http_139_canonical(
                decoded,
                version=version,
                direction="decode",
            )
        return decoded
    _reject_field(
        decoded,
        "socketTimeout",
        version=version,
        direction="decode",
        task_type="HTTP",
        field="task_params.socketTimeout",
        reason="field-absent-in-version",
    )
    return decoded


def _validate_http_139_canonical(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    """Validate the closed HTTP subset that DS 1.3.9 can author exactly."""
    unexpected = sorted(set(payload) - _HTTP_139_CANONICAL_FIELDS)
    if unexpected:
        field = unexpected[0]
        if field == "socketTimeout":
            reason = "native-only-field"
        elif field in {"httpBody", "varPool"}:
            reason = "field-absent-in-version"
        else:
            reason = "outside-reviewed-http-subset"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="HTTP",
            field=f"task_params.{field}",
            reason=reason,
            message=f"HTTP 1.3.9 typed projection does not own field {field!r}",
        )
    try:
        validated = HttpTaskParamsSpec.model_validate(payload)
    except ValidationError as exc:
        error = exc.errors(include_url=False)[0]
        location = error["loc"]
        field = "task_params" + "".join(
            f"[{part}]" if isinstance(part, int) else f".{part}" for part in location
        )
        reason = (
            "missing-required-field"
            if error["type"] == "missing"
            else "invalid-canonical-value"
        )
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="HTTP",
            field=field,
            reason=reason,
            message=f"HTTP 1.3.9 typed task params are invalid: {error['msg']}",
        ) from exc
    parameter_semantics = get_parameter_semantics(version)
    for index, parameter in enumerate(validated.local_params):
        if (
            parameter.direct.value == "OUT"
            and not parameter_semantics.output.var_pool_transport
        ):
            raise _projection_error(
                version=version,
                direction=direction,
                task_type="HTTP",
                field=f"task_params.localParams[{index}].direct",
                reason="unsupported-parameter-direction",
                message="HTTP 1.3.9 typed localParams accept IN direction only",
            )
        if parameter.type.value not in parameter_semantics.allowed_property_types:
            raise _projection_error(
                version=version,
                direction=direction,
                task_type="HTTP",
                field=f"task_params.localParams[{index}].type",
                reason="unsupported-parameter-data-type",
                message=(
                    "HTTP 1.3.9 typed localParams use a data type absent from the "
                    "exact profile"
                ),
            )
