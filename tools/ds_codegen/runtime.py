from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypedDict, cast
from urllib.parse import quote

from dsctl.errors import ApiTransportError
from dsctl.support.json_types import is_json_value

if TYPE_CHECKING:
    from types import ModuleType

    from dsctl.client import (
        DolphinSchedulerClient,
        HttpFormValue,
        HttpQueryParams,
        HttpQueryValue,
        HttpRequestData,
    )
    from dsctl.support.json_types import JsonValue


class RegistryParameterSpec(TypedDict):
    name: str
    java_type: str
    binding: str | None
    wire_name: str | None
    required: bool | None
    default_value: str | None
    hidden: bool


class RegistryDtoFieldSpec(TypedDict):
    name: str
    java_type: str
    wire_name: str
    required: bool | None
    default_value: str | None


class RegistryDtoSpec(TypedDict):
    name: str
    import_path: str
    extends: str | None
    fields: list[RegistryDtoFieldSpec]


class RegistryModelSpec(TypedDict):
    name: str
    import_path: str
    kind: str
    extends: str | None
    fields: list[RegistryDtoFieldSpec]


class RegistryOperationSpec(TypedDict):
    operation_id: str
    controller: str
    method_name: str
    api_group: str
    http_method: str
    path: str
    summary: str | None
    description: str | None
    consumes: list[str]
    return_type: str
    response_projection: str
    parameters: list[RegistryParameterSpec]


RawArguments = Mapping[str, object]
_MISSING = object()
_PATH_VARIABLE_PATTERN = re.compile(r"{([^{}]+)}")
_SERVER_INJECTED_TYPES = {
    "HttpServletRequest",
    "HttpServletResponse",
    "ServletRequest",
    "ServletResponse",
    "User",
}


@dataclass(frozen=True)
class RawContractRegistry:
    operations_by_id: Mapping[str, RegistryOperationSpec]
    dtos_by_name: Mapping[str, RegistryDtoSpec]
    models_by_name: Mapping[str, RegistryModelSpec]

    @classmethod
    def from_module(cls, module: ModuleType) -> RawContractRegistry:
        return cls(
            operations_by_id=cast(
                "Mapping[str, RegistryOperationSpec]",
                module.OPERATIONS_BY_ID,
            ),
            dtos_by_name=cast(
                "Mapping[str, RegistryDtoSpec]",
                module.DTOS_BY_NAME,
            ),
            models_by_name=cast(
                "Mapping[str, RegistryModelSpec]",
                module.MODELS_BY_NAME,
            ),
        )


@dataclass(frozen=True)
class RawInvocationSpec:
    operation_id: str
    http_method: str
    path: str
    query_params: HttpQueryParams | None
    json_body: JsonValue | None
    form_data: HttpRequestData | None
    content: bytes | str | None
    headers: dict[str, str] | None
    unwrap_result: bool
    retryable: bool


def build_raw_invocation(
    registry: RawContractRegistry,
    operation_id: str,
    arguments: RawArguments,
    *,
    unwrap_result: bool | None = None,
    retryable: bool | None = None,
) -> RawInvocationSpec:
    operation = registry.operations_by_id.get(operation_id)
    if operation is None:
        message = "Unknown raw DS operation id"
        raise ApiTransportError(message, details={"operation_id": operation_id})

    consumed_keys: set[str] = set()
    path_values: dict[str, object] = {}
    query_params: dict[str, object] = {}
    form_data: dict[str, object] = {}
    json_body: object | None = None
    content: bytes | str | None = None
    headers: dict[str, str] = {}

    for parameter in operation["parameters"]:
        if _is_server_injected_parameter(parameter):
            continue

        binding = parameter["binding"]
        if binding == "path_variable":
            value = _pop_top_level_argument(arguments, consumed_keys, parameter)
            if value is _MISSING:
                message = "Missing required path variable for raw DS operation"
                raise ApiTransportError(
                    message,
                    details={
                        "operation_id": operation_id,
                        "parameter": parameter["name"],
                        "wire_name": parameter["wire_name"],
                    },
                )
            path_key = parameter["wire_name"] or parameter["name"]
            path_values[path_key] = value
            continue

        if binding == "request_param":
            value = _pop_top_level_argument(arguments, consumed_keys, parameter)
            if value is _MISSING:
                continue
            if _request_params_use_query_string(operation):
                query_params[parameter["wire_name"] or parameter["name"]] = value
            else:
                form_data[parameter["wire_name"] or parameter["name"]] = value
            continue

        if binding == "model_attribute":
            dto_payload = _extract_dto_payload(
                registry=registry,
                dto_name=parameter["java_type"],
                parameter_name=parameter["name"],
                arguments=arguments,
                consumed_keys=consumed_keys,
            )
            if not dto_payload:
                continue
            if operation["http_method"] in {"GET", "DELETE"}:
                query_params.update(dto_payload)
            else:
                form_data.update(dto_payload)
            continue

        if binding == "request_body":
            if json_body is not None or content is not None:
                message = "Raw DS operation includes multiple request bodies"
                raise ApiTransportError(
                    message,
                    details={"operation_id": operation_id},
                )
            body_value, body_headers = _extract_request_body(
                registry=registry,
                operation=operation,
                parameter=parameter,
                arguments=arguments,
                consumed_keys=consumed_keys,
            )
            if body_value is _MISSING:
                continue
            headers.update(body_headers)
            if isinstance(body_value, (bytes, str)):
                content = body_value
            else:
                json_body = body_value
            continue

        message = "Raw DS runtime does not support this parameter binding"
        raise ApiTransportError(
            message,
            details={
                "operation_id": operation_id,
                "parameter": parameter["name"],
                "binding": binding,
                "java_type": parameter["java_type"],
            },
        )

    invocation_path = _render_path(operation["path"], path_values)
    resolved_unwrap_result = (
        _returns_result_envelope(operation, registry)
        if unwrap_result is None
        else unwrap_result
    )
    resolved_retryable = (
        operation["http_method"] == "GET" if retryable is None else retryable
    )
    return RawInvocationSpec(
        operation_id=operation_id,
        http_method=operation["http_method"],
        path=invocation_path,
        query_params=_normalize_query_params(query_params) if query_params else None,
        json_body=(_normalize_json_value(json_body) if json_body is not None else None),
        form_data=_normalize_form_data(form_data) if form_data else None,
        content=content,
        headers=headers or None,
        unwrap_result=resolved_unwrap_result,
        retryable=resolved_retryable,
    )


def invoke_raw_operation(
    client: DolphinSchedulerClient,
    registry: RawContractRegistry,
    operation_id: str,
    arguments: RawArguments,
    *,
    unwrap_result: bool | None = None,
    retryable: bool | None = None,
) -> JsonValue:
    invocation = build_raw_invocation(
        registry,
        operation_id,
        arguments,
        unwrap_result=unwrap_result,
        retryable=retryable,
    )
    request_fn = (
        client.request_result if invocation.unwrap_result else client.request_payload
    )
    return request_fn(
        invocation.http_method,
        invocation.path,
        params=invocation.query_params,
        json_body=invocation.json_body,
        form_data=invocation.form_data,
        content=invocation.content,
        headers=invocation.headers,
        retryable=invocation.retryable,
    )


def _is_server_injected_parameter(parameter: RegistryParameterSpec) -> bool:
    if parameter["hidden"]:
        return True
    if parameter["binding"] == "request_attribute":
        return True
    return parameter["java_type"] in _SERVER_INJECTED_TYPES


def _request_params_use_query_string(operation: RegistryOperationSpec) -> bool:
    if operation["http_method"] in {"GET", "DELETE"}:
        return True
    return any(
        parameter["binding"] == "request_body" for parameter in operation["parameters"]
    )


def _extract_request_body(
    *,
    registry: RawContractRegistry,
    operation: RegistryOperationSpec,
    parameter: RegistryParameterSpec,
    arguments: RawArguments,
    consumed_keys: set[str],
) -> tuple[object, dict[str, str]]:
    direct_value = _pop_top_level_argument(arguments, consumed_keys, parameter)
    headers: dict[str, str] = {}
    if direct_value is not _MISSING:
        normalized_value = _normalize_request_body_value(
            operation=operation,
            parameter=parameter,
            value=direct_value,
        )
        if isinstance(normalized_value, (bytes, str)) and operation["consumes"]:
            headers["Content-Type"] = operation["consumes"][0]
        return normalized_value, headers

    dto_payload = _extract_dto_payload(
        registry=registry,
        dto_name=parameter["java_type"],
        parameter_name=parameter["name"],
        arguments=arguments,
        consumed_keys=consumed_keys,
        allow_flattened_only=True,
    )
    if dto_payload:
        return dto_payload, headers

    return _MISSING, headers


def _normalize_request_body_value(
    *,
    operation: RegistryOperationSpec,
    parameter: RegistryParameterSpec,
    value: object,
) -> object:
    if parameter["java_type"] != "String":
        return value
    if not isinstance(value, (bytes, str)):
        message = "Raw DS request body expected raw string or bytes content"
        raise ApiTransportError(
            message,
            details={
                "operation_id": operation["operation_id"],
                "parameter": parameter["name"],
                "java_type": parameter["java_type"],
            },
        )
    return value


def _extract_dto_payload(
    *,
    registry: RawContractRegistry,
    dto_name: str,
    parameter_name: str,
    arguments: RawArguments,
    consumed_keys: set[str],
    allow_flattened_only: bool = False,
) -> dict[str, object]:
    dto_spec = registry.dtos_by_name.get(dto_name)
    if dto_spec is None:
        return {}

    nested_value = _pop_value_by_aliases(
        arguments,
        consumed_keys,
        [parameter_name],
    )
    if nested_value is not _MISSING:
        if not isinstance(nested_value, Mapping):
            message = "Raw DS DTO argument must be a mapping"
            raise ApiTransportError(
                message,
                details={"dto_name": dto_name, "parameter_name": parameter_name},
            )
        return _normalize_dto_mapping(dto_spec, nested_value)

    if not allow_flattened_only:
        return _extract_flat_dto_payload(dto_spec, arguments, consumed_keys)
    return _extract_flat_dto_payload(dto_spec, arguments, consumed_keys)


def _extract_flat_dto_payload(
    dto_spec: RegistryDtoSpec,
    arguments: RawArguments,
    consumed_keys: set[str],
) -> dict[str, object]:
    payload: dict[str, object] = {}
    for field in dto_spec["fields"]:
        value = _pop_value_by_aliases(
            arguments,
            consumed_keys,
            _candidate_aliases(field["name"], field["wire_name"]),
        )
        if value is _MISSING:
            continue
        payload[field["wire_name"]] = value
    return payload


def _normalize_query_params(raw_query_params: Mapping[str, object]) -> HttpQueryParams:
    cleaned: dict[str, HttpQueryValue] = {}
    for key, value in raw_query_params.items():
        if _is_http_query_scalar(value):
            cleaned[key] = cast("HttpQueryValue", value)
            continue
        if isinstance(value, Sequence) and not isinstance(
            value,
            (str, bytes, bytearray),
        ):
            values = list(value)
            if all(_is_http_query_scalar(item) for item in values):
                cleaned[key] = values
                continue
        message = "Raw DS query params included an unsupported value"
        raise ApiTransportError(
            message,
            details={"parameter": key},
        )
    return cleaned


def _normalize_form_data(raw_form_data: Mapping[str, object]) -> HttpRequestData:
    cleaned: dict[str, HttpFormValue] = {}
    for key, value in raw_form_data.items():
        if value is None or isinstance(value, (str, bytes, int, float, bool)):
            cleaned[key] = value
            continue
        message = "Raw DS form data included an unsupported value"
        raise ApiTransportError(
            message,
            details={"parameter": key},
        )
    return cleaned


def _normalize_json_value(value: object) -> JsonValue:
    if is_json_value(value):
        return value
    message = "Raw DS JSON body must contain only JSON-compatible values"
    raise ApiTransportError(message)


def _is_http_query_scalar(value: object) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def _normalize_dto_mapping(
    dto_spec: RegistryDtoSpec,
    raw_mapping: Mapping[str, object],
) -> dict[str, object]:
    payload: dict[str, object] = {}
    consumed_keys: set[str] = set()
    for field in dto_spec["fields"]:
        value = _pop_value_by_aliases(
            raw_mapping,
            consumed_keys,
            _candidate_aliases(field["name"], field["wire_name"]),
        )
        if value is _MISSING:
            continue
        payload[field["wire_name"]] = value

    extra_keys = sorted(set(raw_mapping.keys()) - consumed_keys)
    if extra_keys:
        message = "Raw DS DTO argument included unsupported fields"
        raise ApiTransportError(
            message,
            details={"dto_name": dto_spec["name"], "extra_keys": extra_keys},
        )
    return payload


def _render_path(path_template: str, path_values: Mapping[str, object]) -> str:
    rendered_path = path_template
    for match in _PATH_VARIABLE_PATTERN.finditer(path_template):
        key = match.group(1)
        if key not in path_values:
            message = "Missing path variable while rendering raw DS operation path"
            raise ApiTransportError(
                message,
                details={"path": path_template, "path_variable": key},
            )
        rendered_value = quote(str(path_values[key]), safe="")
        rendered_path = rendered_path.replace(f"{{{key}}}", rendered_value)
    return rendered_path


def _pop_top_level_argument(
    arguments: RawArguments,
    consumed_keys: set[str],
    parameter: RegistryParameterSpec,
) -> object:
    return _pop_value_by_aliases(
        arguments,
        consumed_keys,
        _candidate_aliases(parameter["name"], parameter["wire_name"]),
    )


def _candidate_aliases(name: str, wire_name: str | None) -> list[str]:
    aliases = [name]
    if wire_name and wire_name not in aliases:
        aliases.append(wire_name)
    return aliases


def _pop_value_by_aliases(
    values: Mapping[str, object],
    consumed_keys: set[str],
    aliases: list[str],
) -> object:
    for alias in aliases:
        if alias in consumed_keys:
            continue
        if alias in values:
            consumed_keys.add(alias)
            return values[alias]
    return _MISSING


def _returns_result_envelope(
    operation: RegistryOperationSpec,
    registry: RawContractRegistry,
) -> bool:
    return _java_type_looks_like_result(
        operation["return_type"],
        registry.models_by_name,
        set(),
    )


def _java_type_looks_like_result(
    java_type: str,
    models_by_name: Mapping[str, RegistryModelSpec],
    seen: set[str],
) -> bool:
    base_name = _base_java_type(java_type)
    if base_name == "Result":
        return True
    if base_name in seen:
        return False
    seen.add(base_name)
    model = models_by_name.get(base_name)
    if model is None or model["extends"] is None:
        return False
    return _java_type_looks_like_result(model["extends"], models_by_name, seen)


def _base_java_type(java_type: str) -> str:
    base_type = java_type.split("<", 1)[0]
    if "." in base_type:
        return base_type.rsplit(".", 1)[-1]
    return base_type
