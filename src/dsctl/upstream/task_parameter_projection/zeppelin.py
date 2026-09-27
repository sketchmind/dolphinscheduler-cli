from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from typing import TYPE_CHECKING

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
    )


def _encode_zeppelin(payload: JsonObject, *, version: str) -> JsonObject:
    """Project stable paragraph intent onto one exact Zeppelin connection wire."""
    encoded = deepcopy(payload)
    surface = get_task_authoring_surface(version).zeppelin
    _require_zeppelin_available(version=version, direction="encode")
    _validate_zeppelin_paragraph_identity(
        encoded,
        version=version,
        direction="encode",
    )
    allowed_fields = {"noteId", "paragraphId", "connectionMode", "parameters"}
    if surface.connection_mode == "REST_ENDPOINT":
        allowed_fields.add("restEndpoint")
    elif surface.connection_mode == "DATASOURCE":
        allowed_fields.add("datasource")
    _reject_zeppelin_unowned_fields(
        encoded,
        allowed_fields=allowed_fields,
        version=version,
        direction="encode",
    )
    mode = encoded.pop("connectionMode", None)
    if mode != surface.connection_mode:
        message = (
            f"ZEPPELIN connectionMode must be {surface.connection_mode} for "
            f"DolphinScheduler {version}"
        )
        raise _projection_error(
            version=version,
            direction="encode",
            task_type="ZEPPELIN",
            field="task_params.connectionMode",
            reason="unsupported-connection-mode",
            message=message,
        )
    if mode == "WORKER_CONFIG":
        _reject_zeppelin_connection_field(
            encoded,
            "restEndpoint",
            version=version,
            direction="encode",
        )
        _reject_zeppelin_connection_field(
            encoded,
            "datasource",
            version=version,
            direction="encode",
        )
    elif mode == "REST_ENDPOINT":
        _require_zeppelin_text_field(
            encoded,
            "restEndpoint",
            version=version,
            direction="encode",
        )
        _reject_zeppelin_connection_field(
            encoded,
            "datasource",
            version=version,
            direction="encode",
        )
    else:
        _reject_zeppelin_connection_field(
            encoded,
            "restEndpoint",
            version=version,
            direction="encode",
        )
        datasource = encoded.get("datasource")
        if (
            not isinstance(datasource, int)
            or isinstance(datasource, bool)
            or datasource <= 0
        ):
            message = "ZEPPELIN datasource must be one positive integer id"
            raise _projection_error(
                version=version,
                direction="encode",
                task_type="ZEPPELIN",
                field="task_params.datasource",
                reason="invalid-datasource-id",
                message=message,
            )
        encoded["type"] = "ZEPPELIN"
    _encode_zeppelin_parameters(
        encoded,
        version=version,
        parameters_supported=surface.literal_parameters,
    )
    return encoded


def _decode_zeppelin(payload: JsonObject, *, version: str) -> JsonObject:
    """Restore one representable native paragraph task to stable connection intent."""
    decoded = deepcopy(payload)
    surface = get_task_authoring_surface(version).zeppelin
    _require_zeppelin_available(version=version, direction="decode")
    _validate_zeppelin_paragraph_identity(
        decoded,
        version=version,
        direction="decode",
    )
    _reject_field(
        decoded,
        "connectionMode",
        version=version,
        direction="decode",
        task_type="ZEPPELIN",
        field="task_params.connectionMode",
        reason="canonical-only-field",
    )
    allowed_fields = {"noteId", "paragraphId"}
    if surface.literal_parameters:
        allowed_fields.add("parameters")
    if surface.connection_mode == "REST_ENDPOINT":
        allowed_fields.add("restEndpoint")
    elif surface.connection_mode == "DATASOURCE":
        allowed_fields.update({"datasource", "type"})
    _reject_zeppelin_unowned_fields(
        decoded,
        allowed_fields=allowed_fields,
        version=version,
        direction="decode",
    )
    if surface.connection_mode == "WORKER_CONFIG":
        _reject_zeppelin_connection_field(
            decoded,
            "restEndpoint",
            version=version,
            direction="decode",
        )
        _reject_zeppelin_connection_field(
            decoded,
            "datasource",
            version=version,
            direction="decode",
        )
        _reject_zeppelin_connection_field(
            decoded,
            "type",
            version=version,
            direction="decode",
        )
    elif surface.connection_mode == "REST_ENDPOINT":
        _require_zeppelin_text_field(
            decoded,
            "restEndpoint",
            version=version,
            direction="decode",
        )
        _reject_zeppelin_connection_field(
            decoded,
            "datasource",
            version=version,
            direction="decode",
        )
        _reject_zeppelin_connection_field(
            decoded,
            "type",
            version=version,
            direction="decode",
        )
    else:
        _reject_zeppelin_connection_field(
            decoded,
            "restEndpoint",
            version=version,
            direction="decode",
        )
        datasource = decoded.get("datasource")
        if (
            not isinstance(datasource, int)
            or isinstance(datasource, bool)
            or datasource <= 0
        ):
            message = "ZEPPELIN native datasource must be one positive integer id"
            raise _projection_error(
                version=version,
                direction="decode",
                task_type="ZEPPELIN",
                field="task_params.datasource",
                reason="invalid-datasource-id",
                message=message,
            )
        native_type = decoded.pop("type", None)
        if native_type != "ZEPPELIN":
            message = "ZEPPELIN datasource wire requires task_params.type=ZEPPELIN"
            raise _projection_error(
                version=version,
                direction="decode",
                task_type="ZEPPELIN",
                field="task_params.type",
                reason="invalid-datasource-type",
                message=message,
            )
    _decode_zeppelin_parameters(
        decoded,
        version=version,
        parameters_supported=surface.literal_parameters,
    )
    decoded["connectionMode"] = surface.connection_mode
    return decoded


def _require_zeppelin_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    if get_task_authoring_surface(version).zeppelin.available:
        return
    message = f"ZEPPELIN does not exist in DolphinScheduler {version}"
    raise _projection_error(
        version=version,
        direction=direction,
        task_type="ZEPPELIN",
        field="task.type",
        reason="task-type-absent-in-version",
        message=message,
    )


def _reject_zeppelin_unowned_fields(
    payload: Mapping[str, JsonValue],
    *,
    allowed_fields: set[str],
    version: str,
    direction: ProjectionDirection,
) -> None:
    unexpected = sorted(set(payload) - allowed_fields)
    if not unexpected:
        return
    names = ", ".join(unexpected)
    message = f"ZEPPELIN paragraph projection does not own fields: {names}"
    raise _projection_error(
        version=version,
        direction=direction,
        task_type="ZEPPELIN",
        field=f"task_params.{unexpected[0]}",
        reason="outside-reviewed-paragraph-subset",
        message=message,
    )


def _validate_zeppelin_paragraph_identity(
    payload: Mapping[str, JsonValue],
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    for key in ("noteId", "paragraphId"):
        value = payload.get(key)
        if not isinstance(value, str) or not value.strip():
            message = f"ZEPPELIN paragraph authoring requires non-empty {key}"
            raise _projection_error(
                version=version,
                direction=direction,
                task_type="ZEPPELIN",
                field=f"task_params.{key}",
                reason="missing-paragraph-identity",
                message=message,
            )


def _reject_zeppelin_connection_field(
    payload: Mapping[str, JsonValue],
    key: str,
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    _reject_field(
        payload,
        key,
        version=version,
        direction=direction,
        task_type="ZEPPELIN",
        field=f"task_params.{key}",
        reason="field-absent-in-connection-epoch",
    )


def _require_zeppelin_text_field(
    payload: Mapping[str, JsonValue],
    key: str,
    *,
    version: str,
    direction: ProjectionDirection,
) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        message = f"ZEPPELIN requires non-empty task_params.{key}"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="ZEPPELIN",
            field=f"task_params.{key}",
            reason="missing-connection-field",
            message=message,
        )
    return value


def _encode_zeppelin_parameters(
    payload: JsonObject,
    *,
    version: str,
    parameters_supported: bool,
) -> None:
    raw_parameters = payload.pop("parameters", None)
    if raw_parameters is None:
        return
    if not isinstance(raw_parameters, Mapping):
        message = "ZEPPELIN canonical parameters must be an object"
        raise _projection_error(
            version=version,
            direction="encode",
            task_type="ZEPPELIN",
            field="task_params.parameters",
            reason="invalid-canonical-parameters",
            message=message,
        )
    parameters = dict(raw_parameters)
    if not parameters:
        return
    if not parameters_supported:
        message = f"ZEPPELIN parameters do not exist in DolphinScheduler {version}"
        raise _projection_error(
            version=version,
            direction="encode",
            task_type="ZEPPELIN",
            field="task_params.parameters",
            reason="field-absent-in-version",
            message=message,
        )
    if any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in parameters.items()
    ):
        message = "ZEPPELIN parameters must contain only string keys and values"
        raise _projection_error(
            version=version,
            direction="encode",
            task_type="ZEPPELIN",
            field="task_params.parameters",
            reason="invalid-canonical-parameters",
            message=message,
        )
    payload["parameters"] = json.dumps(
        parameters,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _decode_zeppelin_parameters(
    payload: JsonObject,
    *,
    version: str,
    parameters_supported: bool,
) -> None:
    raw_parameters = payload.pop("parameters", None)
    if raw_parameters is None or raw_parameters == "":
        payload["parameters"] = {}
        return
    if not parameters_supported:
        message = f"ZEPPELIN parameters do not exist in DolphinScheduler {version}"
        raise _projection_error(
            version=version,
            direction="decode",
            task_type="ZEPPELIN",
            field="task_params.parameters",
            reason="field-absent-in-version",
            message=message,
        )
    if not isinstance(raw_parameters, str):
        message = "ZEPPELIN native parameters must be a JSON object string"
        raise _projection_error(
            version=version,
            direction="decode",
            task_type="ZEPPELIN",
            field="task_params.parameters",
            reason="invalid-native-parameters",
            message=message,
        )
    try:
        parsed = json.loads(
            raw_parameters,
            parse_constant=_reject_zeppelin_json_constant,
            object_pairs_hook=_zeppelin_json_object,
        )
    except (json.JSONDecodeError, ValueError) as exc:
        message = "ZEPPELIN native parameters must contain one strict JSON object"
        raise _projection_error(
            version=version,
            direction="decode",
            task_type="ZEPPELIN",
            field="task_params.parameters",
            reason="invalid-native-parameters",
            message=message,
        ) from exc
    if not isinstance(parsed, dict) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in parsed.items()
    ):
        message = "ZEPPELIN native parameters must contain string keys and values"
        raise _projection_error(
            version=version,
            direction="decode",
            task_type="ZEPPELIN",
            field="task_params.parameters",
            reason="invalid-native-parameters",
            message=message,
        )
    payload["parameters"] = parsed


def _reject_zeppelin_json_constant(value: str) -> JsonValue:
    message = f"Non-standard JSON constant is unsupported: {value}"
    raise ValueError(message)


def _zeppelin_json_object(pairs: list[tuple[str, JsonValue]]) -> JsonObject:
    result: JsonObject = {}
    for key, value in pairs:
        if key in result:
            message = f"Duplicate ZEPPELIN parameter key is unsupported: {key}"
            raise ValueError(message)
        result[key] = value
    return result
