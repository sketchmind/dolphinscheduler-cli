from __future__ import annotations

import keyword
import re
from typing import TYPE_CHECKING, NamedTuple, cast

from ds_codegen.java_literals import parse_java_numeric_literal

if TYPE_CHECKING:
    from pathlib import Path

    from ds_codegen.ir import (
        ContractSnapshot,
        DtoFieldSpec,
        DtoSpec,
        EnumFieldSpec,
        EnumSpec,
        EnumValueSpec,
        ModelSpec,
        OperationSpec,
        ParameterSpec,
    )

_PATH_VARIABLE_PATTERN = re.compile(r"{([^{}]+)}")


class _RenderContext(NamedTuple):
    dtos_by_name: dict[str, DtoSpec]
    models_by_name: dict[str, ModelSpec]
    enums_by_name: dict[str, EnumSpec]


_EMPTY_RENDER_CONTEXT = _RenderContext({}, {}, {})


def render_requests_example(snapshot: ContractSnapshot) -> str:
    operations = {
        operation.operation_id: operation for operation in snapshot.operations
    }
    context = _RenderContext(
        dtos_by_name={dto.name: dto for dto in snapshot.dtos},
        models_by_name={model.name: model for model in snapshot.models},
        enums_by_name={enum_spec.name: enum_spec for enum_spec in snapshot.enums},
    )

    workflow_instance_by_id = operations[
        "WorkflowInstanceV2Controller.queryWorkflowInstanceById"
    ]
    execute_workflow_instance = operations["WorkflowInstanceV2Controller.execute"]
    project_query = operations["ProjectV2Controller.queryProjectListPaging"]
    trigger_workflow = operations["ExecutorController.triggerWorkflowDefinition"]
    create_project = operations["ProjectV2Controller.createProject"]
    create_data_source = operations["DataSourceController.createDataSource"]

    project_query_request = context.dtos_by_name["ProjectQueryRequest"]
    project_create_request = context.dtos_by_name["ProjectCreateRequest"]

    used_enum_names = _collect_used_enum_names(
        context,
        [
            project_query_request,
            project_create_request,
        ],
        [
            trigger_workflow,
            execute_workflow_instance,
            project_query,
            create_project,
            create_data_source,
            workflow_instance_by_id,
        ],
    )
    used_model_names = _collect_used_model_names(
        context,
        [
            project_query_request,
            project_create_request,
        ],
        [
            trigger_workflow,
            execute_workflow_instance,
            project_query,
            create_project,
            create_data_source,
            workflow_instance_by_id,
        ],
    )
    enum_blocks = [
        _render_enum_alias(context.enums_by_name[enum_name])
        for enum_name in sorted(used_enum_names)
    ]
    model_blocks = [
        _render_model_typed_dict(context.models_by_name[model_name], context)
        for model_name in sorted(used_model_names)
    ]

    sections = [
        "from __future__ import annotations",
        "",
        "from enum import Enum, IntEnum, StrEnum",
        "from typing import IO, Any, NotRequired, TypeAlias, TypedDict",
        "",
        "import requests",
        "",
        "# Generated example from the DolphinScheduler 3.4.1 controller contract.",
        "# This example shows the direction for a generated raw client:",
        "# 1. DTOs become TypedDicts",
        "# 2. enums become generated runtime types with DS metadata",
        "# 3. request methods keep DS-native wire names",
        "#    and return the unwrapped data type",
        "# 4. weakly typed upstream responses stay explicit",
        "#    where source metadata is weak",
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
    sections.extend(
        [
            _render_dto_typed_dict(project_query_request, context),
            "",
            _render_dto_typed_dict(project_create_request, context),
            "",
            _render_request_param_typed_dict(
                trigger_workflow,
                typed_dict_name="TriggerWorkflowDefinitionForm",
                context=context,
            ),
            "",
            "class DS341RequestsExampleClient:",
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
            _render_path_only_method(workflow_instance_by_id, context),
            "",
            _render_path_only_method(execute_workflow_instance, context),
            "",
            _render_model_attribute_query_method(
                project_query,
                project_query_request,
                context,
            ),
            "",
            _render_request_param_form_method(
                trigger_workflow,
                payload_type_name="TriggerWorkflowDefinitionForm",
                context=context,
            ),
            "",
            _render_request_body_json_method(
                create_project,
                project_create_request,
                context,
            ),
            "",
            _render_request_body_string_method(create_data_source, context),
            "",
        ]
    )
    return "\n".join(sections)


def write_requests_example(snapshot: ContractSnapshot, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_requests_example(snapshot))


def _collect_used_enum_names(
    context: _RenderContext,
    dtos: list[DtoSpec],
    operations: list[OperationSpec],
) -> set[str]:
    enum_names: set[str] = set()
    for dto in dtos:
        for field in dto.fields:
            enum_names.update(
                _collect_enum_names_from_java_type(field.java_type, context)
            )
    for operation in operations:
        enum_names.update(
            _collect_enum_names_from_java_type(
                _infer_unwrapped_return_type(operation, context),
                context,
            )
        )
        for parameter in operation.parameters:
            if parameter.hidden:
                continue
            enum_names.update(
                _collect_enum_names_from_java_type(parameter.java_type, context)
            )
    return enum_names


def _collect_enum_names_from_java_type(
    java_type: str,
    context: _RenderContext,
) -> set[str]:
    enum_names: set[str] = set()
    base_type = _strip_container(java_type)
    if base_type in context.enums_by_name:
        enum_names.add(base_type)
    generic_base = _generic_base_type(java_type)
    if generic_base in context.enums_by_name:
        enum_names.add(generic_base)
    if java_type.endswith("[]"):
        enum_names.update(_collect_enum_names_from_java_type(java_type[:-2], context))
    if java_type.startswith("List<") and java_type.endswith(">"):
        enum_names.update(_collect_enum_names_from_java_type(java_type[5:-1], context))
    if java_type.startswith("Map<") and java_type.endswith(">"):
        key_type, value_type = _split_generic_pair(java_type[4:-1])
        enum_names.update(_collect_enum_names_from_java_type(key_type, context))
        enum_names.update(_collect_enum_names_from_java_type(value_type, context))
    generic_args = _generic_inner_types(java_type)
    for generic_arg in generic_args:
        enum_names.update(_collect_enum_names_from_java_type(generic_arg, context))
    return enum_names


def _collect_used_model_names(
    context: _RenderContext,
    dtos: list[DtoSpec],
    operations: list[OperationSpec],
) -> set[str]:
    model_names: set[str] = set()
    for dto in dtos:
        for field in dto.fields:
            model_names.update(
                _collect_model_names_from_java_type(field.java_type, context)
            )
    for operation in operations:
        model_names.update(
            _collect_model_names_from_java_type(
                _infer_unwrapped_return_type(operation, context),
                context,
            )
        )
        for parameter in operation.parameters:
            if parameter.hidden:
                continue
            model_names.update(
                _collect_model_names_from_java_type(parameter.java_type, context)
            )
    return model_names


def _collect_model_names_from_java_type(
    java_type: str,
    context: _RenderContext,
) -> set[str]:
    model_names: set[str] = set()
    base_type = _strip_container(java_type)
    if base_type in context.models_by_name:
        model_names.add(base_type)
    generic_base = _generic_base_type(java_type)
    if generic_base in context.models_by_name:
        model_names.add(generic_base)
    if java_type.endswith("[]"):
        model_names.update(_collect_model_names_from_java_type(java_type[:-2], context))
    if java_type.startswith("List<") and java_type.endswith(">"):
        model_names.update(
            _collect_model_names_from_java_type(java_type[5:-1], context)
        )
    if java_type.startswith("Map<") and java_type.endswith(">"):
        key_type, value_type = _split_generic_pair(java_type[4:-1])
        model_names.update(_collect_model_names_from_java_type(key_type, context))
        model_names.update(_collect_model_names_from_java_type(value_type, context))
    generic_args = _generic_inner_types(java_type)
    for generic_arg in generic_args:
        model_names.update(_collect_model_names_from_java_type(generic_arg, context))
    return model_names


def _render_enum_alias(enum_spec: EnumSpec) -> str:
    enum_name = _python_type_name(enum_spec.name)
    wire_field = _enum_wire_field(enum_spec)
    wire_type = (
        _render_python_type(wire_field.java_type, _EMPTY_RENDER_CONTEXT)
        if wire_field is not None
        else "str"
    )
    base_class = _enum_base_class(wire_type)
    lines = [f"class {enum_name}({base_class}):"]
    if not enum_spec.fields:
        lines.extend(
            [
                f"    {value.name} = {_enum_wire_literal(enum_spec, value, wire_type)}"
                for value in enum_spec.values
            ]
        )
        return "\n".join(lines)

    member_fields = [
        (field, _enum_member_attribute_name(field.name)) for field in enum_spec.fields
    ]
    for field, attribute_name in member_fields:
        lines.append(
            f"    {attribute_name}: "
            f"{_render_python_type(field.java_type, _EMPTY_RENDER_CONTEXT)}"
        )
    lines.append("")
    lines.extend(
        _render_enum_new_method(
            enum_spec,
            enum_name,
            base_class,
            wire_type,
            member_fields,
        )
    )

    lines.extend(
        [
            f"    {value.name} = "
            f"{_render_enum_member_constructor(enum_spec, value, wire_type)}"
            for value in enum_spec.values
        ]
    )

    if _enum_supports_from_code(enum_spec):
        lines.append("")
        lines.extend(
            [
                "    @classmethod",
                f'    def from_code(cls, code: int) -> "{enum_name}":',
                "        for member in cls:",
                "            if member.code == code:",
                "                return member",
                f'        raise ValueError(f"Unknown {enum_name} code: {{code}}")',
            ]
        )
    return "\n".join(lines)


def _enum_base_class(wire_type: str) -> str:
    if wire_type == "int":
        return "IntEnum"
    if wire_type == "str":
        return "StrEnum"
    return "Enum"


def _enum_wire_field(enum_spec: EnumSpec) -> EnumFieldSpec | None:
    if enum_spec.json_value_field is None:
        return None
    for field in enum_spec.fields:
        if field.name == enum_spec.json_value_field:
            return field
    return None


def _render_enum_new_method(
    enum_spec: EnumSpec,
    enum_name: str,
    base_class: str,
    wire_type: str,
    member_fields: list[tuple[EnumFieldSpec, str]],
) -> list[str]:
    argument_specs: list[str] = []
    constructor_argument_names: list[str] = []
    wire_field_name = enum_spec.json_value_field
    if wire_field_name is None:
        argument_specs.append(f"wire_value: {wire_type}")
        constructor_argument_names.append("wire_value")
    for field, attribute_name in member_fields:
        argument_name = _enum_member_argument_name(field.name, attribute_name)
        argument_specs.append(
            f"{argument_name}: "
            f"{_render_python_type(field.java_type, _EMPTY_RENDER_CONTEXT)}"
        )
        constructor_argument_names.append(argument_name)

    lines = [
        f"    def __new__(cls, {', '.join(argument_specs)}) -> {enum_name}:",
    ]
    wire_attribute_name = _enum_member_attribute_name(wire_field_name)
    wire_argument_name = (
        _enum_member_argument_name(wire_field_name, wire_attribute_name)
        if wire_field_name is not None
        else "wire_value"
    )
    if base_class == "IntEnum":
        lines.append(f"        obj = int.__new__(cls, {wire_argument_name})")
    elif base_class == "StrEnum":
        lines.append(f"        obj = str.__new__(cls, {wire_argument_name})")
    else:
        lines.append("        obj = object.__new__(cls)")
    lines.append(f"        obj._value_ = {wire_argument_name}")
    for field, attribute_name in member_fields:
        argument_name = _enum_member_argument_name(field.name, attribute_name)
        lines.append(f"        obj.{attribute_name} = {argument_name}")
    lines.append("        return obj")
    return lines


def _enum_member_attribute_name(field_name: str | None) -> str:
    if field_name is None:
        return "wire_value"
    if field_name in {"name", "value"}:
        return f"{field_name}_field"
    return field_name


def _enum_member_argument_name(field_name: str, attribute_name: str) -> str:
    argument_name = attribute_name
    if field_name in {"name", "value"}:
        argument_name = f"{field_name}_arg"
    if keyword.iskeyword(argument_name):
        return f"{argument_name}_"
    return argument_name


def _render_enum_member_constructor(
    enum_spec: EnumSpec,
    value: EnumValueSpec,
    wire_type: str,
) -> str:
    constructor_parts: list[str] = []
    if enum_spec.json_value_field is None:
        constructor_parts.append(_render_python_literal(value.name, wire_type))
    for index, field in enumerate(enum_spec.fields):
        if index >= len(value.arguments):
            break
        constructor_parts.append(
            _render_python_literal(
                value.arguments[index],
                _render_python_type(field.java_type, _EMPTY_RENDER_CONTEXT),
            )
        )
    if len(constructor_parts) == 1:
        return constructor_parts[0]
    return f"({', '.join(constructor_parts)})"


def _enum_supports_from_code(enum_spec: EnumSpec) -> bool:
    code_field = next(
        (
            field
            for field in enum_spec.fields
            if field.name == "code"
            and _render_python_type(field.java_type, _EMPTY_RENDER_CONTEXT) == "int"
        ),
        None,
    )
    return code_field is not None


def _enum_wire_literal(
    enum_spec: EnumSpec,
    value: object,
    wire_type: str,
) -> str:
    enum_value = cast("EnumValueSpec", value)
    argument_index = _enum_field_argument_index(enum_spec, enum_spec.json_value_field)
    if argument_index is None:
        return _render_python_literal(enum_value.name, "str")
    return _render_python_literal(enum_value.arguments[argument_index], wire_type)


def _enum_field_argument_index(
    enum_spec: EnumSpec,
    field_name: str | None,
) -> int | None:
    if field_name is None:
        return None
    for index, field in enumerate(enum_spec.fields):
        if field.name == field_name:
            return index
    return None


def _enum_metadata_value_mapping(
    enum_spec: EnumSpec,
    value: object,
) -> dict[str, str]:
    enum_value = cast("EnumValueSpec", value)
    mapping: dict[str, str] = {}
    for index, field in enumerate(enum_spec.fields):
        if field.name == enum_spec.json_value_field:
            continue
        if index >= len(enum_value.arguments):
            continue
        mapping[field.name] = _render_python_literal(
            enum_value.arguments[index],
            _render_python_type(field.java_type, _EMPTY_RENDER_CONTEXT),
        )
    return mapping


def _render_python_literal(value: str, python_type: str) -> str:
    numeric_value = parse_java_numeric_literal(value)
    if python_type == "int":
        if isinstance(numeric_value, int) and not isinstance(numeric_value, bool):
            return str(numeric_value)
        return repr(value)
    if python_type == "float":
        if isinstance(numeric_value, (int, float)) and not isinstance(
            numeric_value,
            bool,
        ):
            return str(float(numeric_value))
        return repr(value)
    if python_type == "bool":
        return "True" if value.lower() == "true" else "False"
    return repr(value)


def _render_model_typed_dict(model: ModelSpec, context: _RenderContext) -> str:
    lines = [f"class {_python_type_name(model.name)}(TypedDict, total=False):"]
    lines.extend(
        [
            f"    {field.wire_name}: {_render_python_type(field.java_type, context)}"
            for field in model.fields
        ]
    )
    return "\n".join(lines)


def _render_dto_typed_dict(dto: DtoSpec, context: _RenderContext) -> str:
    lines = [f"class {_python_type_name(dto.name)}(TypedDict):"]
    for field in dto.fields:
        python_type = _render_python_type(field.java_type, context)
        if _field_is_required(field):
            lines.append(f"    {field.wire_name}: {python_type}")
        else:
            lines.append(f"    {field.wire_name}: NotRequired[{python_type}]")
    return "\n".join(lines)


def _render_request_param_typed_dict(
    operation: OperationSpec,
    *,
    typed_dict_name: str,
    context: _RenderContext,
) -> str:
    lines = [f"class {typed_dict_name}(TypedDict):"]
    for parameter in operation.parameters:
        if parameter.hidden or parameter.binding != "request_param":
            continue
        python_type = _render_python_type(parameter.java_type, context)
        field_name = parameter.wire_name or parameter.name
        if _parameter_is_required(parameter):
            lines.append(f"    {field_name}: {python_type}")
        else:
            lines.append(f"    {field_name}: NotRequired[{python_type}]")
    return "\n".join(lines)


def _render_path_only_method(
    operation: OperationSpec,
    context: _RenderContext,
) -> str:
    method_name = _snake_case(operation.method_name)
    arguments = _render_path_arguments(operation, context)
    path_template = _render_python_path_template(operation)
    typed_signature = ", ".join(
        f"{argument}: {python_type}" for argument, python_type in arguments
    )
    return_type = _render_python_type(
        _infer_unwrapped_return_type(operation, context),
        context,
    )
    return "\n".join(
        [
            f"    def {method_name}(self, {typed_signature}) -> {return_type}:",
            f'        """{operation.http_method} /{operation.path}"""',
            f'        path = f"{path_template}"',
            f'        return self._request("{operation.http_method}", path)',
        ]
    )


def _render_model_attribute_query_method(
    operation: OperationSpec,
    dto: DtoSpec,
    context: _RenderContext,
) -> str:
    method_name = _snake_case(operation.method_name)
    return_type = _render_python_type(
        _infer_unwrapped_return_type(operation, context),
        context,
    )
    return "\n".join(
        [
            f"    def {method_name}(",
            "        self,",
            f"        request: {_python_type_name(dto.name)},",
            f"    ) -> {return_type}:",
            f'        """{operation.http_method} /{operation.path}"""',
            "        params = self._clean_mapping(dict(request))",
            "        return self._request(",
            f'            "{operation.http_method}",',
            f'            "{operation.path}",',
            "            params=params,",
            "        )",
        ]
    )


def _render_request_param_form_method(
    operation: OperationSpec,
    *,
    payload_type_name: str,
    context: _RenderContext,
) -> str:
    method_name = _snake_case(operation.method_name)
    arguments = _render_path_arguments(operation, context)
    typed_signature = ", ".join(
        f"{argument}: {python_type}" for argument, python_type in arguments
    )
    path_template = _render_python_path_template(operation)
    return_type = _render_python_type(
        _infer_unwrapped_return_type(operation, context),
        context,
    )
    return "\n".join(
        [
            f"    def {method_name}(",
            "        self,",
            f"        {typed_signature},",
            f"        form: {payload_type_name},",
            f"    ) -> {return_type}:",
            f'        """{operation.http_method} /{operation.path}"""',
            '        path = f"' + path_template + '"',
            "        data = self._clean_mapping(dict(form))",
            "        return self._request(",
            f'            "{operation.http_method}",',
            "            path,",
            "            data=data,",
            "        )",
        ]
    )


def _render_request_body_json_method(
    operation: OperationSpec,
    dto: DtoSpec,
    context: _RenderContext,
) -> str:
    method_name = _snake_case(operation.method_name)
    return_type = _render_python_type(
        _infer_unwrapped_return_type(operation, context),
        context,
    )
    return "\n".join(
        [
            f"    def {method_name}(",
            "        self,",
            f"        body: {_python_type_name(dto.name)},",
            f"    ) -> {return_type}:",
            f'        """{operation.http_method} /{operation.path}"""',
            "        return self._request(",
            f'            "{operation.http_method}",',
            f'            "{operation.path}",',
            "            json=body,",
            "        )",
        ]
    )


def _render_request_body_string_method(
    operation: OperationSpec,
    context: _RenderContext,
) -> str:
    method_name = _snake_case(operation.method_name)
    return_type = _render_python_type(
        _infer_unwrapped_return_type(operation, context),
        context,
    )
    return "\n".join(
        [
            f"    def {method_name}(self, raw_json: str) -> {return_type}:",
            f'        """{operation.http_method} /{operation.path}"""',
            "        # DS 3.4.1 models this payload as @RequestBody String jsonStr.",
            "        # Using requests data=... keeps the raw body unwrapped.",
            '        headers = {"Content-Type": "application/json"}',
            "        return self._request(",
            f'            "{operation.http_method}",',
            f'            "{operation.path}",',
            "            data=raw_json,",
            "            headers=headers,",
            "        )",
        ]
    )


def _render_path_arguments(
    operation: OperationSpec,
    context: _RenderContext,
) -> list[tuple[str, str]]:
    arguments: list[tuple[str, str]] = []
    path_parameters = {
        parameter.wire_name or parameter.name: parameter
        for parameter in operation.parameters
        if parameter.binding == "path_variable"
    }
    for placeholder in _PATH_VARIABLE_PATTERN.findall(operation.path):
        parameter = path_parameters.get(placeholder)
        argument_name = _snake_case(placeholder)
        python_type = "int"
        if parameter is not None:
            python_type = _render_python_type(parameter.java_type, context)
        arguments.append((argument_name, python_type))
    return arguments


def _render_python_path_template(operation: OperationSpec) -> str:
    path_template = operation.path
    for placeholder in _PATH_VARIABLE_PATTERN.findall(operation.path):
        path_template = path_template.replace(
            f"{{{placeholder}}}",
            f"{{{_snake_case(placeholder)}}}",
        )
    return path_template


def _field_is_required(field: DtoFieldSpec) -> bool:
    return field.required is True


def _parameter_is_required(parameter: ParameterSpec) -> bool:
    return parameter.required is True or (
        parameter.required is None and parameter.default_value is None
    )


def _infer_unwrapped_return_type(
    operation: OperationSpec,
    context: _RenderContext,
) -> str:
    model = context.models_by_name.get(
        _generic_base_type(operation.logical_return_type)
    )
    if model is None or model.kind != "generated_view" or len(model.fields) != 1:
        return operation.logical_return_type
    field = model.fields[0]
    if field.name not in {"data", "dataList"}:
        return operation.logical_return_type
    return field.java_type


def _render_python_type(java_type: str, context: _RenderContext) -> str:
    scalar_types = {
        "Any": "Any",
        "ArrayNode": "list[object]",
        "Boolean": "bool",
        "Byte": "int",
        "Date": "str",
        "Double": "float",
        "Float": "float",
        "Integer": "int",
        "JsonObject": "JsonObject",
        "JsonValue": "JsonValue",
        "Long": "int",
        "MultipartFile": "UploadFileLike",
        "Object": "object",
        "ObjectNode": "dict[str, object]",
        "Short": "int",
        "String": "str",
        "Void": "None",
        "boolean": "bool",
        "byte": "int",
        "double": "float",
        "float": "float",
        "int": "int",
        "long": "int",
        "short": "int",
        "void": "None",
    }
    if java_type in {"Byte[]", "byte[]"}:
        return "bytes"
    if java_type in scalar_types:
        return scalar_types[java_type]
    if java_type.endswith("[]"):
        return f"list[{_render_python_type(java_type[:-2], context)}]"
    if java_type.startswith("Optional<") and java_type.endswith(">"):
        return f"{_render_python_type(java_type[9:-1], context)} | None"
    if java_type.startswith("List<") and java_type.endswith(">"):
        return f"list[{_render_python_type(java_type[5:-1], context)}]"
    if java_type.startswith("Set<") and java_type.endswith(">"):
        return f"list[{_render_python_type(java_type[4:-1], context)}]"
    if java_type.startswith("Collection<") and java_type.endswith(">"):
        return f"list[{_render_python_type(java_type[11:-1], context)}]"
    if java_type.startswith("Map<") and java_type.endswith(">"):
        key_type, value_type = _split_generic_pair(java_type[4:-1])
        return (
            "dict["
            + _render_python_type(key_type, context)
            + ", "
            + _render_python_type(value_type, context)
            + "]"
        )
    generic_base = _generic_base_type(java_type)
    if generic_base in context.enums_by_name:
        return _python_type_name(generic_base)
    if generic_base in context.dtos_by_name or generic_base in context.models_by_name:
        return _python_type_name(generic_base)
    if java_type in context.enums_by_name:
        return _python_type_name(java_type)
    if java_type in context.dtos_by_name or java_type in context.models_by_name:
        return _python_type_name(java_type)
    return "Any"


def _split_generic_pair(value: str) -> tuple[str, str]:
    depth = 0
    for index, char in enumerate(value):
        if char == "<":
            depth += 1
            continue
        if char == ">":
            depth -= 1
            continue
        if char == "," and depth == 0:
            return value[:index].strip(), value[index + 1 :].strip()
    return value.strip(), "Any"


def _strip_container(java_type: str) -> str:
    if java_type.endswith("[]"):
        return _strip_container(java_type[:-2])
    if java_type.startswith("List<") and java_type.endswith(">"):
        return _strip_container(java_type[5:-1])
    if java_type.startswith("Map<") and java_type.endswith(">"):
        _, value_type = _split_generic_pair(java_type[4:-1])
        return _strip_container(value_type)
    return java_type


def _generic_base_type(java_type: str) -> str:
    if "<" not in java_type or not java_type.endswith(">"):
        return java_type
    return java_type.split("<", 1)[0]


def _generic_inner_types(java_type: str) -> list[str]:
    if "<" not in java_type or not java_type.endswith(">"):
        return []
    inner = java_type.split("<", 1)[1][:-1]
    types: list[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(inner):
        if char == "<":
            depth += 1
            continue
        if char == ">":
            depth -= 1
            continue
        if char == "," and depth == 0:
            types.append(inner[start:index].strip())
            start = index + 1
    types.append(inner[start:].strip())
    return [item for item in types if item]


def _python_type_name(java_type: str) -> str:
    return java_type.replace(".", "_")


def _snake_case(name: str) -> str:
    sanitized_name = re.sub(r"[^0-9A-Za-z]+", "_", name)
    parts: list[str] = []
    for index, char in enumerate(sanitized_name):
        if char.isupper() and index > 0 and not sanitized_name[index - 1].isupper():
            parts.append("_")
        parts.append(char.lower())
    return "".join(parts)
