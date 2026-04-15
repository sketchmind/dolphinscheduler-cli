from __future__ import annotations

import re
from typing import TYPE_CHECKING

from ds_codegen.render.requests_example import (
    _generic_base_type,
    _generic_inner_types,
    _infer_unwrapped_return_type,
    _parameter_is_required,
    _python_type_name,
    _render_enum_alias,
    _render_path_arguments,
    _render_python_path_template,
    _RenderContext,
    _snake_case,
    _split_generic_pair,
)
from ds_codegen.render.requests_example import (
    _render_python_type as _render_base_python_type,
)

if TYPE_CHECKING:
    from pathlib import Path

    from ds_codegen.ir import (
        ContractSnapshot,
        DtoFieldSpec,
        DtoSpec,
        ModelSpec,
        OperationSpec,
        ParameterSpec,
    )

_SERVER_INJECTED_TYPES = {
    "HttpServletRequest",
    "HttpServletResponse",
    "HttpSession",
    "ServletRequest",
    "ServletResponse",
    "User",
}


def _collect_used_enum_names(
    snapshot: ContractSnapshot,
    context: _RenderContext,
    used_dto_names: set[str],
    used_model_names: set[str],
    specialized_model_types: set[str],
) -> set[str]:
    used_enum_names: set[str] = set()
    for operation in snapshot.operations:
        used_enum_names.update(
            _collect_enum_names_from_java_type(
                _infer_unwrapped_return_type(operation, context),
                context,
            )
        )
        for parameter in operation.parameters:
            if parameter.hidden:
                continue
            used_enum_names.update(
                _collect_enum_names_from_java_type(parameter.java_type, context)
            )
    for dto_name in used_dto_names:
        for field in context.dtos_by_name[dto_name].fields:
            used_enum_names.update(
                _collect_enum_names_from_java_type(field.java_type, context)
            )
    for model_name in used_model_names:
        for field in context.models_by_name[model_name].fields:
            used_enum_names.update(
                _collect_enum_names_from_java_type(field.java_type, context)
            )
    for specialized_java_type in specialized_model_types:
        substitutions = _generic_model_substitutions(specialized_java_type)
        base_model = context.models_by_name[_generic_base_type(specialized_java_type)]
        for field in base_model.fields:
            used_enum_names.update(
                _collect_enum_names_from_java_type(
                    _substitute_type_parameters(field.java_type, substitutions),
                    context,
                )
            )
    return used_enum_names


def _collect_enum_names_from_java_type(
    java_type: str,
    context: _RenderContext,
) -> set[str]:
    used_enum_names: set[str] = set()
    generic_base = _generic_base_type(java_type)
    if generic_base in context.enums_by_name:
        used_enum_names.add(generic_base)
    if java_type.endswith("[]"):
        used_enum_names.update(
            _collect_enum_names_from_java_type(java_type[:-2], context)
        )
    for generic_arg in _generic_inner_types(java_type):
        used_enum_names.update(_collect_enum_names_from_java_type(generic_arg, context))
    return used_enum_names


def _collect_used_dto_names(
    snapshot: ContractSnapshot,
    context: _RenderContext,
) -> set[str]:
    used_dto_names: set[str] = set()
    for operation in snapshot.operations:
        for parameter in _visible_operation_parameters(operation):
            generic_base = _generic_base_type(parameter.java_type)
            if generic_base in context.dtos_by_name:
                used_dto_names.add(generic_base)
    return used_dto_names


def _collect_used_model_names(
    snapshot: ContractSnapshot,
    context: _RenderContext,
) -> set[str]:
    used_model_names: set[str] = set()
    pending_java_types: list[str] = []
    for operation in snapshot.operations:
        pending_java_types.append(_infer_unwrapped_return_type(operation, context))
        pending_java_types.extend(
            parameter.java_type
            for parameter in _visible_operation_parameters(operation)
        )
    for dto_name in _collect_used_dto_names(snapshot, context):
        dto = context.dtos_by_name[dto_name]
        pending_java_types.extend(field.java_type for field in dto.fields)

    while pending_java_types:
        java_type = pending_java_types.pop()
        generic_base = _generic_base_type(java_type)
        if (
            generic_base in context.models_by_name
            and generic_base not in used_model_names
        ):
            used_model_names.add(generic_base)
            pending_java_types.extend(
                field.java_type for field in context.models_by_name[generic_base].fields
            )
        if java_type.endswith("[]"):
            pending_java_types.append(java_type[:-2])
        if java_type.startswith("Map<") and java_type.endswith(">"):
            key_type, value_type = _split_generic_pair(java_type[4:-1])
            pending_java_types.append(key_type)
            pending_java_types.append(value_type)
            continue
        pending_java_types.extend(_generic_inner_types(java_type))
    return used_model_names


def _collect_specialized_model_types(
    snapshot: ContractSnapshot,
    context: _RenderContext,
    used_dto_names: set[str],
    used_model_names: set[str],
) -> set[str]:
    specialized_types: set[str] = set()
    pending_java_types: list[str] = []
    for operation in snapshot.operations:
        pending_java_types.append(_infer_unwrapped_return_type(operation, context))
        pending_java_types.extend(
            parameter.java_type
            for parameter in _visible_operation_parameters(operation)
        )
    for dto_name in used_dto_names:
        dto = context.dtos_by_name[dto_name]
        pending_java_types.extend(field.java_type for field in dto.fields)
    for model_name in used_model_names:
        model = context.models_by_name[model_name]
        pending_java_types.extend(field.java_type for field in model.fields)

    visited_java_types: set[str] = set()
    while pending_java_types:
        java_type = pending_java_types.pop()
        if java_type in visited_java_types:
            continue
        visited_java_types.add(java_type)
        generic_base = _generic_base_type(java_type)
        generic_args = _generic_inner_types(java_type)
        if generic_args and generic_base in context.models_by_name:
            specialized_types.add(java_type)
            substitutions = _generic_model_substitutions(java_type)
            pending_java_types.extend(
                _substitute_type_parameters(field.java_type, substitutions)
                for field in context.models_by_name[generic_base].fields
            )
            continue
        if java_type.endswith("[]"):
            pending_java_types.append(java_type[:-2])
            continue
        if java_type.startswith("Map<") and java_type.endswith(">"):
            key_type, value_type = _split_generic_pair(java_type[4:-1])
            pending_java_types.append(key_type)
            pending_java_types.append(value_type)
            continue
        pending_java_types.extend(generic_args)
    return specialized_types


def render_requests_client(snapshot: ContractSnapshot) -> str:
    context = _RenderContext(
        dtos_by_name={dto.name: dto for dto in snapshot.dtos},
        models_by_name={model.name: model for model in snapshot.models},
        enums_by_name={enum_spec.name: enum_spec for enum_spec in snapshot.enums},
    )
    used_dto_names = _collect_used_dto_names(snapshot, context)
    used_model_names = _collect_used_model_names(snapshot, context)
    specialized_model_types = _collect_specialized_model_types(
        snapshot,
        context,
        used_dto_names,
        used_model_names,
    )
    used_enum_names = _collect_used_enum_names(
        snapshot,
        context,
        used_dto_names,
        used_model_names,
        specialized_model_types,
    )
    enum_blocks = [
        _render_enum_alias(context.enums_by_name[enum_name])
        for enum_name in sorted(used_enum_names)
    ]
    model_blocks = [
        _render_model_typed_dict(
            context.models_by_name[model_name],
            context,
            specialized_model_types,
        )
        for model_name in sorted(used_model_names)
        if not _model_is_generic_base(
            context.models_by_name[model_name],
            specialized_model_types,
        )
    ]
    model_blocks.extend(
        [
            _render_specialized_model_typed_dict(
                specialized_java_type,
                context,
                specialized_model_types,
            )
            for specialized_java_type in sorted(specialized_model_types)
        ]
    )
    dto_blocks = [
        _render_dto_typed_dict(
            context.dtos_by_name[dto_name],
            context,
            specialized_model_types,
        )
        for dto_name in sorted(used_dto_names)
    ]
    request_param_blocks = [
        _render_request_param_typed_dict(
            operation,
            context,
            specialized_model_types,
        )
        for operation in snapshot.operations
        if _visible_request_params(operation)
    ]
    method_blocks = [
        _render_operation_method(
            operation,
            context,
            specialized_model_types,
        )
        for operation in snapshot.operations
    ]

    sections = [
        "from __future__ import annotations",
        "",
        "from enum import Enum, IntEnum, StrEnum",
        "from typing import IO, Any, NotRequired, TypeAlias, TypedDict",
        "",
        "import requests",
        "",
        "# Generated raw requests client from the DolphinScheduler 3.4.1",
        "# controller contract. This layer stays close to DS-native routes,",
        "# parameter names, and return shapes.",
        "",
        "UploadFileContent: TypeAlias = IO[bytes] | bytes",
        "UploadFileLike: TypeAlias = (",
        "    UploadFileContent",
        "    | tuple[str, UploadFileContent]",
        "    | tuple[str, UploadFileContent, str]",
        "    | tuple[str, UploadFileContent, str, dict[str, str]]",
        ")",
        "JsonScalar: TypeAlias = str | int | float | bool | None",
        'JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | '
        'dict[str, "JsonValue"]',
        "JsonObject: TypeAlias = dict[str, JsonValue]",
        "",
    ]
    if enum_blocks:
        sections.extend(enum_blocks)
        sections.append("")
    if model_blocks:
        sections.extend(model_blocks)
        sections.append("")
    if dto_blocks:
        sections.extend(dto_blocks)
        sections.append("")
    if request_param_blocks:
        sections.extend(request_param_blocks)
        sections.append("")
    sections.extend(
        [
            "class DS341RequestsClient:",
            "    def __init__(",
            "        self,",
            "        base_url: str,",
            "        token: str,",
            "        *,",
            "        session: requests.Session | None = None,",
            "    ) -> None:",
            '        self.base_url = base_url.rstrip("/")',
            "        self.token = token",
            "        self._session = session or requests.Session()",
            "",
            "    def _default_headers(self) -> dict[str, str]:",
            '        return {"accept": "application/json", "token": self.token}',
            "",
            "    def _clean_mapping(",
            "        self,",
            "        values: dict[str, Any],",
            "    ) -> dict[str, Any]:",
            "        return {",
            "            key: value",
            "            for key, value in values.items()",
            "            if value is not None",
            "        }",
            "",
            "    def _request(self, method: str, path: str, **kwargs: Any) -> Any:",
            "        headers = self._default_headers()",
            '        extra_headers = kwargs.pop("headers", None)',
            "        if extra_headers:",
            "            headers.update(extra_headers)",
            "        url = f\"{self.base_url}/{path.lstrip('/')}\"",
            "        response = self._session.request(",
            "            method,",
            "            url,",
            "            headers=headers,",
            "            **kwargs,",
            "        )",
            "        response.raise_for_status()",
            "        payload = response.json()",
            "        if isinstance(payload, dict) and {",
            '            "code", "msg"',
            "        }.issubset(",
            "            payload",
            "        ):",
            '            if "data" not in payload and "dataList" not in payload:',
            "                return payload",
            '            data = payload.get("data")',
            '            if data is None and "data" not in payload:',
            '                data = payload.get("dataList")',
            '            if payload.get("code") != 0:',
            "                raise RuntimeError(",
            "                    f\"DS API error {payload.get('code')}: \"",
            "                    f\"{payload.get('msg')}\"",
            "                )",
            "            return data",
            "        return payload",
            "",
        ]
    )
    sections.extend(method_blocks)
    sections.append("")
    return "\n".join(sections)


def write_requests_client(snapshot: ContractSnapshot, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_requests_client(snapshot))


def _visible_operation_parameters(
    operation: OperationSpec,
) -> list[ParameterSpec]:
    visible_parameters: list[ParameterSpec] = []
    for parameter in operation.parameters:
        if parameter.hidden or parameter.binding == "request_attribute":
            continue
        if parameter.binding is None and parameter.java_type in _SERVER_INJECTED_TYPES:
            continue
        visible_parameters.append(parameter)
    return visible_parameters


def _visible_request_params(operation: OperationSpec) -> list[ParameterSpec]:
    return [
        parameter
        for parameter in _visible_operation_parameters(operation)
        if parameter.binding == "request_param"
    ]


def _request_param_typed_dict_name(operation: OperationSpec) -> str:
    sanitized = re.sub(
        r"_+",
        "_",
        re.sub(r"[^0-9A-Za-z_]+", "_", _python_type_name(operation.operation_id)),
    ).strip("_")
    sanitized = sanitized.removesuffix("Request")
    return f"{sanitized}Params"


def _request_params_use_query_string(operation: OperationSpec) -> bool:
    return operation.http_method in {"DELETE", "GET"}


def _render_request_param_typed_dict(
    operation: OperationSpec,
    context: _RenderContext,
    specialized_model_types: set[str],
) -> str:
    fields = [
        (
            parameter.wire_name or parameter.name,
            _render_client_python_type(
                parameter.java_type,
                context,
                specialized_model_types,
            ),
            _parameter_is_required(parameter),
        )
        for parameter in _visible_request_params(operation)
    ]
    return _render_functional_typed_dict(
        _request_param_typed_dict_name(operation),
        fields,
    )


def _operation_method_name(operation: OperationSpec) -> str:
    sanitized_operation_id = re.sub(
        r"[^0-9A-Za-z_]+",
        "_",
        operation.operation_id,
    )
    return re.sub(
        r"_+",
        "_",
        _snake_case(sanitized_operation_id),
    ).strip("_")


def _explicit_content_type(
    operation: OperationSpec,
    *,
    request_bodies: list[ParameterSpec],
) -> str | None:
    if not request_bodies:
        if operation.consumes:
            return operation.consumes[0]
        return None

    request_body = request_bodies[0]
    if request_body.java_type == "String":
        if operation.consumes:
            return operation.consumes[0]
        return "application/json"
    return None


def _render_operation_method(
    operation: OperationSpec,
    context: _RenderContext,
    specialized_model_types: set[str],
) -> str:
    visible_parameters = _visible_operation_parameters(operation)
    unsupported_parameters = [
        parameter
        for parameter in visible_parameters
        if parameter.binding
        not in {
            "model_attribute",
            "path_variable",
            "request_body",
            "request_param",
        }
    ]
    if unsupported_parameters:
        unsupported_types = ", ".join(
            f"{parameter.name}:{parameter.binding}:{parameter.java_type}"
            for parameter in unsupported_parameters
        )
        message = (
            f"Unsupported generated parameter bindings for "
            f"{operation.operation_id}: {unsupported_types}"
        )
        raise ValueError(message)

    path_arguments = _render_path_arguments(operation, context)
    request_params = [
        parameter
        for parameter in visible_parameters
        if parameter.binding == "request_param"
    ]
    model_attributes = [
        parameter
        for parameter in visible_parameters
        if parameter.binding == "model_attribute"
    ]
    request_bodies = [
        parameter
        for parameter in visible_parameters
        if parameter.binding == "request_body"
    ]
    if len(model_attributes) > 1 or len(request_bodies) > 1:
        message = f"Unsupported multi-payload operation {operation.operation_id}"
        raise ValueError(message)

    signature_items = ["self"]
    for argument_name, python_type in path_arguments:
        signature_items.append(f"{argument_name}: {python_type}")
    if request_params:
        payload_argument_name = (
            "params" if _request_params_use_query_string(operation) else "form"
        )
        signature_items.append(
            f"{payload_argument_name}: {_request_param_typed_dict_name(operation)}"
        )
    if model_attributes:
        model_attribute = model_attributes[0]
        signature_items.append(
            "request: "
            + _render_client_python_type(
                model_attribute.java_type,
                context,
                specialized_model_types,
            )
        )
    if request_bodies:
        request_body = request_bodies[0]
        signature_items.append(
            f"{_snake_case(request_body.name)}: "
            + _render_client_python_type(
                request_body.java_type,
                context,
                specialized_model_types,
            )
        )

    return_type = _render_client_python_type(
        _infer_unwrapped_return_type(operation, context),
        context,
        specialized_model_types,
    )
    lines = [
        f"    def {_operation_method_name(operation)}(",
        "        " + (",\n        ".join(signature_items)),
        f"    ) -> {return_type}:",
        (
            f'        """{operation.operation_id} | '
            f'{operation.http_method} /{operation.path}"""'
        ),
    ]
    if path_arguments:
        lines.append(f'        path = f"{_render_python_path_template(operation)}"')
        request_path = "path"
    else:
        request_path = f'"{operation.path}"'

    explicit_content_type = _explicit_content_type(
        operation,
        request_bodies=request_bodies,
    )
    request_keyword_lines: list[str] = []
    if request_params:
        payload_argument_name = (
            "params" if _request_params_use_query_string(operation) else "form"
        )
        request_keyword_name = (
            "params" if _request_params_use_query_string(operation) else "data"
        )
        payload_variable_name = (
            "query_params" if _request_params_use_query_string(operation) else "data"
        )
        lines.append(
            f"        {payload_variable_name} = "
            f"self._clean_mapping(dict({payload_argument_name}))"
        )
        request_keyword_lines.append(
            f"            {request_keyword_name}={payload_variable_name},"
        )
    if model_attributes:
        payload_variable_name = (
            "query_params" if _request_params_use_query_string(operation) else "data"
        )
        request_payload_line = "self._clean_mapping(dict(request))"
        if request_params:
            lines.append(
                f"        {payload_variable_name}.update({request_payload_line})"
            )
        else:
            lines.append(f"        {payload_variable_name} = {request_payload_line}")
            request_keyword_lines.append(
                f"            {payload_variable_name}={payload_variable_name},"
            )
    if request_bodies:
        request_body = request_bodies[0]
        body_argument_name = _snake_case(request_body.name)
        if request_body.java_type == "String":
            if explicit_content_type is None:
                message = "String request bodies must render one explicit content type"
                raise ValueError(message)
            lines.append(
                f'        headers = {{"Content-Type": "{explicit_content_type}"}}'
            )
            request_keyword_lines.append(f"            data={body_argument_name},")
            request_keyword_lines.append("            headers=headers,")
        else:
            request_keyword_lines.append(f"            json={body_argument_name},")
    elif explicit_content_type is not None:
        lines.append(f'        headers = {{"Content-Type": "{explicit_content_type}"}}')
        request_keyword_lines.append("            headers=headers,")

    if not request_keyword_lines:
        lines.append(
            f'        return self._request("{operation.http_method}", {request_path})'
        )
        return "\n".join(lines)

    lines.extend(
        [
            "        return self._request(",
            f'            "{operation.http_method}",',
            f"            {request_path},",
            *request_keyword_lines,
            "        )",
        ]
    )
    return "\n".join(lines)


def _render_model_typed_dict(
    model: ModelSpec,
    context: _RenderContext,
    specialized_model_types: set[str],
) -> str:
    fields = [
        (
            field.wire_name,
            _render_client_python_type(
                field.java_type,
                context,
                specialized_model_types,
            ),
            False,
        )
        for field in model.fields
    ]
    return _render_functional_typed_dict(
        _python_type_name(model.name),
        fields,
        total=False,
    )


def _render_dto_typed_dict(
    dto: DtoSpec,
    context: _RenderContext,
    specialized_model_types: set[str],
) -> str:
    fields = [
        (
            field.wire_name,
            _render_client_python_type(
                field.java_type,
                context,
                specialized_model_types,
            ),
            _field_is_required(field),
        )
        for field in dto.fields
    ]
    return _render_functional_typed_dict(
        _python_type_name(dto.name),
        fields,
    )


def _render_functional_typed_dict(
    name: str,
    fields: list[tuple[str, str, bool]],
    *,
    total: bool = True,
) -> str:
    fields = _dedupe_typed_dict_fields(fields)
    lines = [
        f"{name} = TypedDict(",
        f'    "{name}",',
        "    {",
    ]
    for field_name, python_type, required in fields:
        rendered_type = python_type if required else f"NotRequired[{python_type}]"
        lines.append(f'        "{field_name}": {rendered_type},')
    lines.append("    },")
    if not total:
        lines.append("    total=False,")
    lines.append(")")
    return "\n".join(lines)


def _dedupe_typed_dict_fields(
    fields: list[tuple[str, str, bool]],
) -> list[tuple[str, str, bool]]:
    merged_fields: dict[str, tuple[str, bool]] = {}
    ordered_field_names: list[str] = []
    for field_name, python_type, required in fields:
        if field_name not in merged_fields:
            merged_fields[field_name] = (python_type, required)
            ordered_field_names.append(field_name)
            continue
        existing_type, existing_required = merged_fields[field_name]
        merged_fields[field_name] = (
            _prefer_python_type(existing_type, python_type),
            existing_required or required,
        )
    return [
        (field_name, *merged_fields[field_name]) for field_name in ordered_field_names
    ]


def _prefer_python_type(existing_type: str, new_type: str) -> str:
    if _is_weak_python_type(existing_type) and not _is_weak_python_type(new_type):
        return new_type
    return existing_type


def _is_weak_python_type(python_type: str) -> bool:
    return python_type in {
        "Any",
        "dict[str, Any]",
        "list[Any]",
    }


def _field_is_required(field: DtoFieldSpec) -> bool:
    return field.required is True


def _render_specialized_model_typed_dict(
    specialized_java_type: str,
    context: _RenderContext,
    specialized_model_types: set[str],
) -> str:
    generic_base = _generic_base_type(specialized_java_type)
    base_model = context.models_by_name[generic_base]
    substitutions = _generic_model_substitutions(specialized_java_type)
    fields = [
        (
            field.wire_name,
            _render_client_python_type(
                _substitute_type_parameters(field.java_type, substitutions),
                context,
                specialized_model_types,
            ),
            False,
        )
        for field in base_model.fields
    ]
    return _render_functional_typed_dict(
        _specialized_python_type_name(specialized_java_type),
        fields,
        total=False,
    )


def _render_client_python_type(
    java_type: str,
    context: _RenderContext,
    specialized_model_types: set[str],
) -> str:
    if java_type in {"Byte[]", "byte[]"}:
        return "bytes"
    if java_type.endswith("[]"):
        inner_type = _render_client_python_type(
            java_type[:-2],
            context,
            specialized_model_types,
        )
        return f"list[{inner_type}]"
    if java_type.startswith("Optional<") and java_type.endswith(">"):
        inner_type = _render_client_python_type(
            java_type[9:-1],
            context,
            specialized_model_types,
        )
        return f"{inner_type} | None"
    if java_type.startswith("List<") and java_type.endswith(">"):
        inner_type = _render_client_python_type(
            java_type[5:-1],
            context,
            specialized_model_types,
        )
        return f"list[{inner_type}]"
    if java_type.startswith("Set<") and java_type.endswith(">"):
        inner_type = _render_client_python_type(
            java_type[4:-1],
            context,
            specialized_model_types,
        )
        return f"list[{inner_type}]"
    if java_type.startswith("Collection<") and java_type.endswith(">"):
        inner_type = _render_client_python_type(
            java_type[11:-1],
            context,
            specialized_model_types,
        )
        return f"list[{inner_type}]"
    if java_type.startswith("Map<") and java_type.endswith(">"):
        key_type, value_type = _split_generic_pair(java_type[4:-1])
        rendered_key_type = _render_client_python_type(
            key_type,
            context,
            specialized_model_types,
        )
        rendered_value_type = _render_client_python_type(
            value_type,
            context,
            specialized_model_types,
        )
        return "dict[" + rendered_key_type + ", " + rendered_value_type + "]"
    if java_type in specialized_model_types:
        return _specialized_python_type_name(java_type)
    generic_base = _generic_base_type(java_type)
    if generic_base in context.models_by_name and java_type in specialized_model_types:
        return _specialized_python_type_name(java_type)
    return _render_base_python_type(java_type, context)


def _model_is_generic_base(
    model: ModelSpec,
    specialized_model_types: set[str],
) -> bool:
    if all("T" not in field.java_type for field in model.fields):
        return False
    return any(
        _generic_base_type(specialized_java_type) == model.name
        for specialized_java_type in specialized_model_types
    )


def _generic_model_substitutions(java_type: str) -> dict[str, str]:
    generic_args = _generic_inner_types(java_type)
    if len(generic_args) == 1:
        return {"T": generic_args[0]}
    return {}


def _substitute_type_parameters(
    java_type: str,
    substitutions: dict[str, str],
) -> str:
    if java_type in substitutions:
        return substitutions[java_type]
    if java_type.endswith("[]"):
        return _substitute_type_parameters(java_type[:-2], substitutions) + "[]"
    if "<" not in java_type or not java_type.endswith(">"):
        return substitutions.get(java_type, java_type)
    generic_base = _generic_base_type(java_type)
    rendered_args = ", ".join(
        _substitute_type_parameters(generic_arg, substitutions)
        for generic_arg in _generic_inner_types(java_type)
    )
    return f"{generic_base}<{rendered_args}>"


def _specialized_python_type_name(java_type: str) -> str:
    return re.sub(
        r"_+",
        "_",
        re.sub(r"[^0-9A-Za-z_]+", "_", _python_type_name(java_type)),
    ).strip("_")
