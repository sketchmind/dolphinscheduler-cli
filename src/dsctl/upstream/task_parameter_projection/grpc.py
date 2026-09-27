from __future__ import annotations

import json
from copy import deepcopy
from types import MappingProxyType
from typing import TYPE_CHECKING

from pydantic import ValidationError

from dsctl.models.task_spec import (
    GrpcLiteralUnaryStringRecordTaskParamsSpec,
)
from dsctl.support.json_types import JsonObject, JsonValue, require_json_object
from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _projection_error,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from dsctl.upstream.task_parameter_projection.types import (
        ProjectionDirection,
        TaskParameterProjectionError,
    )

_GRPC_WIRE_FIELDS = frozenset(
    {
        "url",
        "channelCredentialType",
        "grpcServiceDefinition",
        "grpcServiceDefinitionJSON",
        "methodName",
        "message",
        "grpcCheckCondition",
        "condition",
        "grpcConnectTimeoutMs",
    }
)
_GRPC_WIRE_FIELD_ORDER = (
    "url",
    "channelCredentialType",
    "grpcServiceDefinition",
    "grpcServiceDefinitionJSON",
    "methodName",
    "message",
    "grpcCheckCondition",
    "condition",
    "grpcConnectTimeoutMs",
)
_GRPC_MODEL_FIELD_ALIASES: Mapping[str, str] = MappingProxyType(
    {
        "channel_credential_type": "channelCredentialType",
        "service_name": "serviceName",
        "method_name": "methodName",
        "request_fields": "requestFields",
        "response_fields": "responseFields",
        "grpc_connect_timeout_ms": "grpcConnectTimeoutMs",
    }
)


def _encode_grpc(payload: JsonObject, *, version: str) -> JsonObject:
    """Generate one deterministic unary GRPC wire from closed canonical intent."""
    _require_grpc_available(version=version, direction="encode")
    validated = _validate_grpc_canonical(
        payload,
        version=version,
        direction="encode",
    )
    return _grpc_native_wire(validated)


def _decode_grpc(payload: JsonObject, *, version: str) -> JsonObject:
    """Decode only an exact fixed-point unary GRPC string-record wire."""
    _require_grpc_available(version=version, direction="decode")
    _require_exact_grpc_wire_fields(payload, version=version)
    descriptor_value = payload["grpcServiceDefinitionJSON"]
    if not isinstance(descriptor_value, str):
        raise _grpc_projection_error(
            version=version,
            direction="decode",
            field="task_params.grpcServiceDefinitionJSON",
            reason="invalid-native-descriptor",
            message="GRPC native protobufjs descriptor must be one JSON string",
        )
    service_name, method_name, request_fields, response_fields = (
        _decode_grpc_descriptor(descriptor_value, version=version)
    )
    message_value = payload["message"]
    if not isinstance(message_value, str):
        raise _grpc_projection_error(
            version=version,
            direction="decode",
            field="task_params.message",
            reason="invalid-native-message",
            message="GRPC native message must be one compact JSON object string",
        )
    message = _parse_grpc_json_object(
        message_value,
        version=version,
        field="task_params.message",
        reason="invalid-native-message",
        label="message",
    )
    canonical: JsonObject = {
        "url": deepcopy(payload["url"]),
        "channelCredentialType": deepcopy(payload["channelCredentialType"]),
        "serviceName": service_name,
        "methodName": method_name,
        "requestFields": request_fields,
        "responseFields": response_fields,
        "message": message,
        "grpcConnectTimeoutMs": deepcopy(payload["grpcConnectTimeoutMs"]),
    }
    validated = _validate_grpc_canonical(
        canonical,
        version=version,
        direction="decode",
    )
    expected = _grpc_native_wire(validated)
    for key in _GRPC_WIRE_FIELD_ORDER:
        if payload[key] == expected[key]:
            continue
        raise _grpc_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{key}",
            reason="non-canonical-native-wire",
            message=(
                f"GRPC native field {key!r} does not match the deterministic "
                "literal unary string-record wire"
            ),
        )
    return require_json_object(
        validated.to_payload(),
        label="validated GRPC task_params",
    )


def _validate_grpc_canonical(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> GrpcLiteralUnaryStringRecordTaskParamsSpec:
    """Apply the closed GRPC model and translate failures to projection errors."""
    try:
        return GrpcLiteralUnaryStringRecordTaskParamsSpec.model_validate(payload)
    except ValidationError as exc:
        error = exc.errors(include_url=False)[0]
        message = str(error["msg"])
        field = _grpc_validation_field(error["loc"], message=message)
        reason = (
            "missing-required-field"
            if error["type"] == "missing"
            else "outside-reviewed-literal-unary-string-record-subset"
            if error["type"] == "extra_forbidden"
            else "invalid-canonical-value"
        )
        raise _grpc_projection_error(
            version=version,
            direction=direction,
            field=field,
            reason=reason,
            message=f"GRPC typed task params are invalid: {message}",
        ) from exc
    except TypeError as exc:
        message = str(exc)
        raise _grpc_projection_error(
            version=version,
            direction=direction,
            field=_grpc_validation_field((), message=message),
            reason="invalid-canonical-value",
            message=f"GRPC typed task params are invalid: {message}",
        ) from exc


def _grpc_validation_field(
    location: Sequence[str | int],
    *,
    message: str,
) -> str:
    """Translate Pydantic locations and root validators to canonical aliases."""
    if location:
        return "task_params" + "".join(
            (
                f"[{part}]"
                if isinstance(part, int)
                else f".{_GRPC_MODEL_FIELD_ALIASES.get(part, part)}"
            )
            for part in location
        )
    for field_name in (
        "serviceName",
        "methodName",
        "message",
        "requestFields",
        "responseFields",
        "channelCredentialType",
        "grpcConnectTimeoutMs",
        "url",
    ):
        if field_name in message:
            return f"task_params.{field_name}"
    return "task_params"


def _grpc_native_wire(
    validated: GrpcLiteralUnaryStringRecordTaskParamsSpec,
) -> JsonObject:
    """Build the sole deterministic native wire accepted by the reviewed facet."""
    request_lines = "\n".join(
        f"  string {field.name} = {field.number};" for field in validated.request_fields
    )
    response_lines = "\n".join(
        f"  string {field.name} = {field.number};"
        for field in validated.response_fields
    )
    proto = (
        'syntax = "proto3";\n\n'
        f"service {validated.service_name} {{\n"
        f"  rpc {validated.method_name} (Request) returns (Response);\n"
        "}\n\n"
        f"message Request {{\n{request_lines}\n}}\n\n"
        f"message Response {{\n{response_lines}\n}}\n"
    )
    request_descriptor: JsonObject = {
        field.name: {"type": "string", "id": field.number}
        for field in validated.request_fields
    }
    response_descriptor: JsonObject = {
        field.name: {"type": "string", "id": field.number}
        for field in validated.response_fields
    }
    descriptor: JsonObject = {
        "nested": {
            validated.service_name: {
                "methods": {
                    validated.method_name: {
                        "requestType": "Request",
                        "responseType": "Response",
                    }
                }
            },
            "Request": {"fields": request_descriptor},
            "Response": {"fields": response_descriptor},
        }
    }
    return {
        "url": validated.url,
        "channelCredentialType": validated.channel_credential_type,
        "grpcServiceDefinition": proto,
        "grpcServiceDefinitionJSON": json.dumps(
            descriptor,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "methodName": f"{validated.service_name}/{validated.method_name}",
        "message": json.dumps(
            validated.message,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ),
        "grpcCheckCondition": "STATUS_CODE_DEFAULT",
        "condition": "",
        "grpcConnectTimeoutMs": validated.grpc_connect_timeout_ms,
    }


def _require_exact_grpc_wire_fields(payload: JsonObject, *, version: str) -> None:
    """Reject missing and richer GRPC native packages before typed decoding."""
    unexpected = sorted(set(payload) - _GRPC_WIRE_FIELDS)
    if unexpected:
        raise _grpc_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-literal-unary-string-record-subset",
            message=(
                "GRPC literal unary string-record projection does not own fields: "
                f"{', '.join(unexpected)}"
            ),
        )
    missing = sorted(_GRPC_WIRE_FIELDS - set(payload))
    if missing:
        raise _grpc_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{missing[0]}",
            reason="missing-required-wire-field",
            message=(
                f"GRPC native wire is missing required fields: {', '.join(missing)}"
            ),
        )


def _decode_grpc_descriptor(
    raw_descriptor: str,
    *,
    version: str,
) -> tuple[str, str, list[JsonObject], list[JsonObject]]:
    """Extract canonical record intent from one exact protobufjs descriptor shape."""
    descriptor = _parse_grpc_json_object(
        raw_descriptor,
        version=version,
        field="task_params.grpcServiceDefinitionJSON",
        reason="invalid-native-descriptor",
        label="protobufjs descriptor",
    )
    nested = _grpc_exact_object_member(
        descriptor,
        member="nested",
        exact_keys=None,
        version=version,
    )
    service_names = sorted(set(nested) - {"Request", "Response"})
    if len(nested) != 3 or len(service_names) != 1:
        raise _grpc_descriptor_error(
            version=version,
            message=(
                "GRPC protobufjs descriptor must contain exactly one service plus "
                "Request and Response records"
            ),
        )
    service_name = service_names[0]
    service = _grpc_exact_object_value(
        nested[service_name],
        exact_keys={"methods"},
        version=version,
        label="service",
    )
    methods = _grpc_exact_object_member(
        service,
        member="methods",
        exact_keys=None,
        version=version,
    )
    if len(methods) != 1:
        raise _grpc_descriptor_error(
            version=version,
            message="GRPC protobufjs descriptor must contain exactly one RPC method",
        )
    method_name = next(iter(methods))
    method = _grpc_exact_object_value(
        methods[method_name],
        exact_keys={"requestType", "responseType"},
        version=version,
        label="method",
    )
    if method != {"requestType": "Request", "responseType": "Response"}:
        raise _grpc_descriptor_error(
            version=version,
            message="GRPC protobufjs method must be unary Request to Response",
        )
    request = _grpc_descriptor_record_fields(
        nested.get("Request"),
        version=version,
        label="Request",
    )
    response = _grpc_descriptor_record_fields(
        nested.get("Response"),
        version=version,
        label="Response",
    )
    return service_name, method_name, request, response


def _grpc_descriptor_record_fields(
    value: JsonValue,
    *,
    version: str,
    label: str,
) -> list[JsonObject]:
    """Decode one flat protobufjs string-record field map without normalization."""
    record = _grpc_exact_object_value(
        value,
        exact_keys={"fields"},
        version=version,
        label=label,
    )
    fields = _grpc_exact_object_member(
        record,
        member="fields",
        exact_keys=None,
        version=version,
    )
    decoded: list[JsonObject] = []
    for name, field_value in fields.items():
        field = _grpc_exact_object_value(
            field_value,
            exact_keys={"type", "id"},
            version=version,
            label=f"{label} field",
        )
        if field.get("type") != "string":
            raise _grpc_descriptor_error(
                version=version,
                message=f"GRPC {label} descriptor fields must use string type",
            )
        decoded.append({"name": name, "number": deepcopy(field["id"])})
    return decoded


def _grpc_exact_object_member(
    payload: JsonObject,
    *,
    member: str,
    exact_keys: set[str] | None,
    version: str,
) -> JsonObject:
    """Require one descriptor member to be an object with an optional exact shape."""
    if member not in payload:
        raise _grpc_descriptor_error(
            version=version,
            message=f"GRPC protobufjs descriptor is missing {member!r}",
        )
    return _grpc_exact_object_value(
        payload[member],
        exact_keys=exact_keys,
        version=version,
        label=member,
    )


def _grpc_exact_object_value(
    value: JsonValue,
    *,
    exact_keys: set[str] | None,
    version: str,
    label: str,
) -> JsonObject:
    """Require one parsed descriptor value to be a JSON object of the exact shape."""
    if not isinstance(value, dict) or (
        exact_keys is not None and set(value) != exact_keys
    ):
        raise _grpc_descriptor_error(
            version=version,
            message=f"GRPC protobufjs descriptor {label} has an invalid shape",
        )
    return require_json_object(value, label=f"GRPC protobufjs descriptor {label}")


def _parse_grpc_json_object(
    raw: str,
    *,
    version: str,
    field: str,
    reason: str,
    label: str,
) -> JsonObject:
    """Parse strict JSON objects while rejecting duplicate keys and extensions."""
    try:
        parsed: JsonValue = json.loads(
            raw,
            parse_constant=_reject_grpc_json_constant,
            object_pairs_hook=_grpc_json_object,
        )
    except (json.JSONDecodeError, ValueError) as exc:
        raise _grpc_projection_error(
            version=version,
            direction="decode",
            field=field,
            reason=reason,
            message=f"GRPC native {label} must be one strict JSON object string",
        ) from exc
    if not isinstance(parsed, dict):
        raise _grpc_projection_error(
            version=version,
            direction="decode",
            field=field,
            reason=reason,
            message=f"GRPC native {label} must be one JSON object",
        )
    return require_json_object(parsed, label=f"GRPC native {label}")


def _reject_grpc_json_constant(value: str) -> JsonValue:
    message = f"Non-standard JSON constant is unsupported: {value}"
    raise ValueError(message)


def _grpc_json_object(pairs: list[tuple[str, JsonValue]]) -> JsonObject:
    result: JsonObject = {}
    for key, value in pairs:
        if key in result:
            message = f"Duplicate GRPC JSON key is unsupported: {key}"
            raise ValueError(message)
        result[key] = value
    return result


def _require_grpc_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    if get_task_authoring_surface(version).grpc.available:
        return
    raise _grpc_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=f"GRPC does not exist in DolphinScheduler {version}",
    )


def _grpc_descriptor_error(
    *,
    version: str,
    message: str,
) -> TaskParameterProjectionError:
    return _grpc_projection_error(
        version=version,
        direction="decode",
        field="task_params.grpcServiceDefinitionJSON",
        reason="invalid-native-descriptor",
        message=message,
    )


def _grpc_projection_error(
    *,
    version: str,
    direction: ProjectionDirection,
    field: str,
    reason: str,
    message: str,
) -> TaskParameterProjectionError:
    return _projection_error(
        version=version,
        direction=direction,
        task_type="GRPC",
        field=field,
        reason=reason,
        message=message,
    )
