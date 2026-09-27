"""Render generated operation modules with explicit helper dependencies."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ds_codegen.contract_type_refs import generic_base_type
from ds_codegen.contract_visibility import (
    has_executable_response_type,
    is_client_supplied_parameter,
    is_required_parameter,
)
from ds_codegen.operation_paths import operation_path_arguments
from ds_codegen.render.package.render_support import (
    render_parameter_default_annotation,
    snake_case,
)
from ds_codegen.render.package.surface_renderer import (
    controller_module_name,
    controller_operations_class_name,
)
from ds_codegen.snapshot_resolution import ResolutionScope

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from ds_codegen.ir import OperationSpec, ParameterSpec, ReferenceOwnerKind
    from ds_codegen.render.package.planner import PackageRenderContext

AnnotationImportTarget = tuple[tuple[str, ...], str]
_STRICT_INT_LIST_RESPONSE_OPERATION_IDS = frozenset(
    {"TaskDefinitionController.genTaskCodeList"}
)
_STRICT_INT_RESPONSE_OPERATION_IDS = frozenset(
    {
        "TaskDefinitionController.updateTaskDefinition",
        "TaskDefinitionController.updateTaskWithUpstream",
    }
)


@dataclass(frozen=True)
class OperationRenderDeps:
    """Inject shared helper behavior without coupling back to the parent renderer."""

    requests_base_class_name: str
    base_params_model_name: str
    collect_annotation_import_targets: Callable[..., set[AnnotationImportTarget]]
    display_doc_text: Callable[[str], str]
    field_annotation_type: Callable[..., str]
    pydantic_field_name: Callable[[str], str]
    python_class_name: Callable[[str], str]
    relative_import_statement: Callable[[tuple[str, ...], tuple[str, ...], str], str]
    render_annotation_type: Callable[..., str]
    render_docstring_lines: Callable[..., list[str]]
    render_parameter_field_config: Callable[..., str | None]


@dataclass(frozen=True)
class OperationResponseUse:
    """Exact payload type selected after applying an operation projection."""

    java_type: str
    owner_kind: ReferenceOwnerKind
    owner_ref: str


def _visible_operation_parameters(
    operation: OperationSpec,
) -> list[ParameterSpec]:
    return [
        parameter
        for parameter in operation.parameters
        if is_client_supplied_parameter(parameter)
    ]


def _visible_request_params(operation: OperationSpec) -> list[ParameterSpec]:
    return [
        parameter
        for parameter in _visible_operation_parameters(operation)
        if parameter.binding == "request_param"
    ]


def _render_python_path_template(operation: OperationSpec) -> str:
    path_template = operation.path
    for placeholder in re.findall(r"\{([^{}]+)\}", operation.path):
        path_template = path_template.replace(
            f"{{{placeholder}}}",
            f"{{{snake_case(placeholder)}}}",
        )
    return path_template


def _explicit_content_type(
    operation: OperationSpec,
    *,
    request_bodies: list[ParameterSpec],
) -> str | None:
    if not request_bodies:
        return operation.consumes[0] if operation.consumes else None
    request_body = request_bodies[0]
    if request_body.java_type != "String":
        return None
    return operation.consumes[0] if operation.consumes else "application/json"


def write_operations_modules(
    package_root: Path,
    context: PackageRenderContext,
    package_exports: dict[tuple[str, ...], dict[str, list[str]]],
    *,
    deps: OperationRenderDeps,
) -> None:
    operations_by_controller: dict[str, list[OperationSpec]] = defaultdict(list)
    for operation in context.snapshot.operations:
        operations_by_controller[operation.controller].append(operation)

    for controller_name, operations in sorted(operations_by_controller.items()):
        module_name = controller_module_name(controller_name)
        module_path = package_root / "api" / "operations" / f"{module_name}.py"
        module_path.parent.mkdir(parents=True, exist_ok=True)
        module_path.write_text(
            render_operations_module(
                controller_name=controller_name,
                module_name=module_name,
                operations=operations,
                context=context,
                deps=deps,
            )
        )
        package_exports[("api", "operations")][module_name] = [
            controller_operations_class_name(controller_name)
        ]


def render_operations_module(
    *,
    controller_name: str,
    module_name: str,
    operations: list[OperationSpec],
    context: PackageRenderContext,
    deps: OperationRenderDeps,
) -> str:
    operation_class_name = controller_operations_class_name(controller_name)
    uses_request_params = any(
        _visible_request_params(operation) for operation in operations
    )
    needs_upload_alias = any(
        _module_uses_upload_alias(operation) for operation in operations
    )
    uses_binary_response = any(
        _is_binary_response_operation(operation) for operation in operations
    )
    return_uses = {
        operation.operation_id: operation_response_use(operation, context)
        for operation in operations
    }
    annotation_uses = [
        (parameter.java_type, operation.operation_id, "operation_request")
        for operation in operations
        for parameter in _visible_operation_parameters(operation)
    ]
    annotation_uses.extend(
        (
            return_uses[operation.operation_id].java_type,
            return_uses[operation.operation_id].owner_ref,
            return_uses[operation.operation_id].owner_kind,
        )
        for operation in operations
        if not _is_binary_response_operation(operation)
    )
    uses_json_value = any(
        "JsonValue"
        in deps.render_annotation_type(
            java_type,
            owner_import_path=owner_ref,
            context=context,
            owner_kind=owner_kind,
        )
        for java_type, owner_ref, owner_kind in annotation_uses
    )
    base_import_names = [deps.requests_base_class_name]
    if uses_request_params:
        base_import_names.append(deps.base_params_model_name)
    if needs_upload_alias:
        base_import_names.append("UploadFileLike")
    if uses_binary_response:
        base_import_names.append("BinaryPayload")
    if uses_json_value:
        base_import_names.append("JsonValue")
    base_import = f"from ._base import {', '.join(base_import_names)}"
    uses_validated_returns = any(
        not _is_binary_response_operation(operation)
        and deps.render_annotation_type(
            return_uses[operation.operation_id].java_type,
            owner_import_path=return_uses[operation.operation_id].owner_ref,
            context=context,
            owner_kind=return_uses[operation.operation_id].owner_kind,
        )
        != "None"
        for operation in operations
    )
    uses_strict_int_list_response = any(
        operation.operation_id in _STRICT_INT_LIST_RESPONSE_OPERATION_IDS
        for operation in operations
    )
    uses_strict_int_response = any(
        operation.operation_id in _STRICT_INT_RESPONSE_OPERATION_IDS
        for operation in operations
    )

    sections = [
        "from __future__ import annotations",
        "",
        base_import,
        "",
    ]
    pydantic_imports: list[str] = []
    if uses_request_params:
        pydantic_imports.append("Field")
    if uses_strict_int_list_response or uses_strict_int_response:
        pydantic_imports.append("StrictInt")
    if uses_validated_returns:
        pydantic_imports.append("TypeAdapter")
    if pydantic_imports:
        sections.extend([f"from pydantic import {', '.join(pydantic_imports)}", ""])

    import_targets: set[AnnotationImportTarget] = set()
    current_module_parts = ("api", "operations", module_name)
    for operation in operations:
        for parameter in _visible_request_params(operation):
            import_targets.update(
                deps.collect_annotation_import_targets(
                    parameter.java_type,
                    owner_import_path=operation.operation_id,
                    current_module_parts=current_module_parts,
                    context=context,
                    owner_kind="operation_request",
                )
            )
        for parameter in _visible_operation_parameters(operation):
            if parameter.binding not in {
                "model_attribute",
                "request_body",
                "path_variable",
            }:
                continue
            import_targets.update(
                deps.collect_annotation_import_targets(
                    parameter.java_type,
                    owner_import_path=operation.operation_id,
                    current_module_parts=current_module_parts,
                    context=context,
                    owner_kind="operation_request",
                )
            )
        if not _is_binary_response_operation(operation):
            import_targets.update(
                deps.collect_annotation_import_targets(
                    return_uses[operation.operation_id].java_type,
                    owner_import_path=return_uses[operation.operation_id].owner_ref,
                    current_module_parts=current_module_parts,
                    context=context,
                    owner_kind=return_uses[operation.operation_id].owner_kind,
                )
            )
    if import_targets:
        sections.extend(
            sorted(
                deps.relative_import_statement(
                    current_module_parts,
                    target_module_parts,
                    class_name,
                )
                for target_module_parts, class_name in import_targets
            )
        )
        sections.append("")

    request_param_blocks = [
        render_operation_request_params(operation, context, deps=deps)
        for operation in operations
        if _visible_request_params(operation)
    ]
    if request_param_blocks:
        sections.append("\n\n".join(request_param_blocks))
        sections.append("")

    method_blocks = [
        _render_operation_method(
            operation,
            context,
            deps=deps,
        )
        for operation in operations
    ]
    sections.append(f"class {operation_class_name}({deps.requests_base_class_name}):")
    if method_blocks:
        for method_block in method_blocks:
            sections.append(method_block)
            sections.append("")
    else:
        sections.append("    pass")
        sections.append("")
    sections.append(f'__all__ = ["{operation_class_name}"]')
    sections.append("")
    return "\n".join(sections)


def _module_uses_upload_alias(operation: OperationSpec) -> bool:
    for parameter in _visible_operation_parameters(operation):
        if "MultipartFile" in parameter.java_type:
            return True
    return False


def render_operation_request_params(
    operation: OperationSpec,
    context: PackageRenderContext,
    *,
    deps: OperationRenderDeps,
    class_name: str | None = None,
    include_docstring: bool = True,
    honor_parameter_defaults: bool = False,
) -> str:
    class_name = class_name or _operation_request_params_class_name(
        operation,
        deps=deps,
    )
    lines = [f"class {class_name}({deps.base_params_model_name}):"]
    if include_docstring:
        lines.extend(
            deps.render_docstring_lines(
                _operation_params_docstring(operation, deps=deps),
                indent="    ",
            )
        )
    for parameter in _visible_request_params(operation):
        is_required = is_required_parameter(parameter)
        attribute_name = deps.pydantic_field_name(parameter.wire_name or parameter.name)
        rendered_type = deps.field_annotation_type(
            parameter.java_type,
            allow_none=not is_required,
            owner_import_path=operation.operation_id,
            context=context,
            owner_kind="operation_request",
        )
        field_config = deps.render_parameter_field_config(
            parameter,
            required=is_required,
            attribute_name=attribute_name,
            honor_default_value=honor_parameter_defaults,
        )
        if honor_parameter_defaults and not is_required:
            rendered_type = render_parameter_default_annotation(
                parameter, rendered_type
            )
        if field_config is None:
            if is_required:
                lines.append(f"    {attribute_name}: {rendered_type}")
            else:
                lines.append(f"    {attribute_name}: {rendered_type} = None")
            continue
        lines.append(f"    {attribute_name}: {rendered_type} = Field({field_config})")
    if len(lines) == 1:
        lines.append("    pass")
    return "\n".join(lines)


def _operation_request_params_class_name(
    operation: OperationSpec,
    *,
    deps: OperationRenderDeps,
) -> str:
    suffix = operation.operation_id.split(".", 1)[1]
    class_base_name = deps.python_class_name(suffix).removesuffix("Request")
    return f"{class_base_name}Params"


def _render_operation_method(
    operation: OperationSpec,
    context: PackageRenderContext,
    *,
    deps: OperationRenderDeps,
) -> str:
    method_name = _operation_method_name(operation)
    visible_parameters = _visible_operation_parameters(operation)
    path_arguments = _render_operation_path_arguments(
        operation,
        context,
        deps=deps,
    )
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

    signature_items = ["self"]
    for argument_name, python_type in path_arguments:
        signature_items.append(f"{argument_name}: {python_type}")
    if request_params:
        payload_arg_name = (
            "params" if operation.http_method in {"DELETE", "GET"} else "form"
        )
        signature_items.append(
            f"{payload_arg_name}: "
            f"{_operation_request_params_class_name(operation, deps=deps)}"
        )
    if model_attributes:
        model_attribute = model_attributes[0]
        signature_items.append(
            "request: "
            + deps.render_annotation_type(
                model_attribute.java_type,
                owner_import_path=operation.operation_id,
                context=context,
                owner_kind="operation_request",
            )
        )
    if request_bodies:
        request_body = request_bodies[0]
        signature_items.append(
            f"{snake_case(request_body.name)}: "
            + deps.render_annotation_type(
                request_body.java_type,
                owner_import_path=operation.operation_id,
                context=context,
                owner_kind="operation_request",
            )
        )

    is_binary_response = _is_binary_response_operation(operation)
    return_use = operation_response_use(operation, context)
    return_type = (
        "BinaryPayload"
        if is_binary_response
        else deps.render_annotation_type(
            return_use.java_type,
            owner_import_path=return_use.owner_ref,
            context=context,
            owner_kind=return_use.owner_kind,
        )
    )
    payload_adapter_type = response_adapter_annotation(
        operation,
        return_type=return_type,
    )
    lines = [
        f"    def {method_name}(",
        "        " + ",\n        ".join(signature_items),
    ]
    method_docstring = _operation_method_docstring(
        operation,
        path_argument_names=path_arguments,
        request_params_arg_name=(
            "params"
            if request_params and operation.http_method in {"DELETE", "GET"}
            else "form"
            if request_params
            else None
        ),
        has_model_attributes=bool(model_attributes),
        has_request_bodies=bool(request_bodies),
        deps=deps,
    )
    lines.append(f"    ) -> {return_type}:")
    lines.extend(deps.render_docstring_lines(method_docstring, indent="        "))
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
        payload_arg_name = (
            "params" if operation.http_method in {"DELETE", "GET"} else "form"
        )
        request_keyword_name = (
            "params" if operation.http_method in {"DELETE", "GET"} else "data"
        )
        payload_var_name = (
            "query_params" if operation.http_method in {"DELETE", "GET"} else "data"
        )
        multipart_fields = [
            parameter
            for parameter in request_params
            if "MultipartFile" in parameter.java_type
        ]
        if multipart_fields:
            if operation.http_method in {"DELETE", "GET"}:
                message = (
                    f"multipart operation {operation.operation_id} cannot use "
                    f"{operation.http_method}"
                )
                raise ValueError(message)
            field_items = ", ".join(
                f"{deps.pydantic_field_name(parameter.wire_name or parameter.name)!r}: "
                f"{(parameter.wire_name or parameter.name)!r}"
                for parameter in multipart_fields
            )
            lines.append(
                "        data, files = self._multipart_mapping("
                f"{payload_arg_name}, file_fields={{{field_items}}})"
            )
            request_keyword_lines.extend(
                [
                    "            data=data,",
                    "            files=files,",
                ]
            )
        else:
            lines.append(
                f"        {payload_var_name} = self._model_mapping({payload_arg_name})"
            )
            request_keyword_lines.append(
                f"            {request_keyword_name}={payload_var_name},"
            )
    if model_attributes:
        payload_var_name = (
            "query_params" if operation.http_method in {"DELETE", "GET"} else "data"
        )
        request_keyword_name = (
            "params" if operation.http_method in {"DELETE", "GET"} else "data"
        )
        payload_expr = "self._model_mapping(request)"
        if request_params:
            lines.append(f"        {payload_var_name}.update({payload_expr})")
        else:
            lines.append(f"        {payload_var_name} = {payload_expr}")
            request_keyword_lines.append(
                f"            {request_keyword_name}={payload_var_name},"
            )
    if request_bodies:
        request_body = request_bodies[0]
        body_arg_name = snake_case(request_body.name)
        if request_body.java_type == "String":
            if explicit_content_type is None:
                message = "String request bodies must render one explicit content type"
                raise ValueError(message)
            request_keyword_lines.append(f"            content={body_arg_name},")
            lines.append(
                f'        headers = {{"Content-Type": "{explicit_content_type}"}}'
            )
            request_keyword_lines.append("            headers=headers,")
        else:
            request_keyword_lines.append(
                f"            json=self._json_payload({body_arg_name}),"
            )
    elif explicit_content_type is not None:
        lines.append(f'        headers = {{"Content-Type": "{explicit_content_type}"}}')
        request_keyword_lines.append("            headers=headers,")

    if not request_keyword_lines:
        request_expr = _request_expression(
            operation,
            request_path=request_path,
            is_binary_response=is_binary_response,
        )
        if is_binary_response:
            lines.append(f"        return {request_expr}")
        elif return_type == "None":
            lines.append(f"        {request_expr}")
            lines.append("        return None")
        else:
            lines.append(f"        payload = {request_expr}")
            lines.extend(_projected_payload_lines(operation))
            lines.append(
                "        "
                "return self._validate_payload("
                f"payload, TypeAdapter({payload_adapter_type})"
                ")"
            )
        return "\n".join(lines)

    request_call_lines = _request_call_lines(
        operation,
        request_path=request_path,
        request_keyword_lines=request_keyword_lines,
        is_binary_response=is_binary_response,
    )
    if is_binary_response:
        lines.append(f"        return {request_call_lines[0]}")
        lines.extend(f"        {line}" for line in request_call_lines[1:])
        return "\n".join(lines)
    if return_type == "None":
        lines.append("        " + request_call_lines[0])
        lines.extend(f"        {line}" for line in request_call_lines[1:])
        lines.append("        return None")
        return "\n".join(lines)

    lines.append(f"        payload = {request_call_lines[0]}")
    lines.extend(f"        {line}" for line in request_call_lines[1:-1])
    lines.append(f"        {request_call_lines[-1]}")
    lines.extend(_projected_payload_lines(operation))
    lines.append(
        "        return self._validate_payload("
        f"payload, TypeAdapter({payload_adapter_type})"
        ")"
    )
    return "\n".join(lines)


def _request_expression(
    operation: OperationSpec,
    *,
    request_path: str,
    is_binary_response: bool,
) -> str:
    request_helper = "_request_binary" if is_binary_response else "_request"
    return f'self.{request_helper}("{operation.http_method}", {request_path})'


def _request_call_lines(
    operation: OperationSpec,
    *,
    request_path: str,
    request_keyword_lines: list[str],
    is_binary_response: bool,
) -> list[str]:
    return [
        f"self.{'_request_binary' if is_binary_response else '_request'}(",
        f'    "{operation.http_method}",',
        f"    {request_path},",
        *[line.strip() for line in request_keyword_lines],
        ")",
    ]


def _render_operation_path_arguments(
    operation: OperationSpec,
    context: PackageRenderContext,
    *,
    deps: OperationRenderDeps,
) -> list[tuple[str, str]]:
    arguments: list[tuple[str, str]] = []
    for argument in operation_path_arguments(operation):
        parameter = argument.parameter
        annotation = (
            "int"
            if parameter is None
            else deps.render_annotation_type(
                parameter.java_type,
                owner_import_path=operation.operation_id,
                context=context,
                owner_kind="operation_request",
            )
        )
        arguments.append((snake_case(argument.name), annotation))
    return arguments


def _projected_payload_lines(operation: OperationSpec) -> list[str]:
    if operation.response_projection == "status_data":
        return ["        payload = self._project_status_data(payload)"]
    if operation.response_projection == "single_data":
        return ["        payload = self._project_single_data(payload)"]
    if operation.response_projection == "single_data_list":
        return ["        payload = self._project_single_data_list(payload)"]
    return []


def response_adapter_annotation(
    operation: OperationSpec,
    *,
    return_type: str,
) -> str:
    """Render endpoint-specific response validation without widening annotations."""
    if operation.operation_id in _STRICT_INT_LIST_RESPONSE_OPERATION_IDS:
        if return_type != "list[int]":
            message = (
                f"Strict integer-list response override for {operation.operation_id} "
                f"expected list[int], got {return_type}"
            )
            raise ValueError(message)
        return "list[StrictInt]"
    if operation.operation_id in _STRICT_INT_RESPONSE_OPERATION_IDS:
        if return_type == "int":
            return "StrictInt"
        if return_type == "int | None":
            return "StrictInt | None"
        message = (
            f"Strict integer response override for {operation.operation_id} "
            f"expected int or int | None, got {return_type}"
        )
        raise ValueError(message)
    return return_type


def _operation_method_name(operation: OperationSpec) -> str:
    suffix = operation.operation_id.split(".", 1)[1]
    return snake_case(suffix)


def _operation_params_docstring(
    operation: OperationSpec,
    *,
    deps: OperationRenderDeps,
) -> str:
    title = _preferred_operation_title(operation, deps=deps)
    description_parts = [title]
    if operation.description and deps.display_doc_text(operation.description) != title:
        description_parts.append(deps.display_doc_text(operation.description))
    payload_kind = "Query" if operation.http_method in {"DELETE", "GET"} else "Form"
    description_parts.append(f"{payload_kind} parameters for {operation.operation_id}.")
    return "\n\n".join(description_parts)


def _preferred_operation_title(
    operation: OperationSpec,
    *,
    deps: OperationRenderDeps,
) -> str:
    if operation.summary:
        if " " in operation.summary:
            return deps.display_doc_text(operation.summary)
        return _humanize_identifier(operation.summary)
    if operation.documentation:
        first_paragraph = operation.documentation.split("\n\n", 1)[0]
        return deps.display_doc_text(first_paragraph)
    return operation.operation_id


def _humanize_identifier(value: str) -> str:
    spaced = re.sub(r"[_-]+", " ", value)
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", spaced)
    tokens = [token for token in spaced.split() if token]
    if not tokens:
        return value
    return " ".join(
        token if token.isupper() else token.capitalize() for token in tokens
    )


def _operation_method_docstring(
    operation: OperationSpec,
    *,
    path_argument_names: list[tuple[str, str]],
    request_params_arg_name: str | None,
    has_model_attributes: bool,
    has_request_bodies: bool,
    deps: OperationRenderDeps,
) -> str:
    title = _preferred_operation_title(operation, deps=deps)
    sections = [title]
    detail_parts = [
        part
        for part in (operation.description, operation.documentation)
        if part is not None and deps.display_doc_text(part) != title
    ]
    detail_parts = [deps.display_doc_text(part) for part in detail_parts]
    source_method = f"{operation.controller}.{operation.method_name}"
    operation_metadata = (
        f"DS operation: {source_method} | {operation.http_method} /{operation.path}"
    )
    if operation.operation_id != source_method:
        operation_metadata += f"\nDS operation ID: {operation.operation_id}"
    detail_parts.append(operation_metadata)
    sections.append("\n\n".join(detail_parts))

    arg_lines: list[str] = []
    path_argument_docs = {
        snake_case(parameter.name): parameter.description
        for parameter in operation.parameters
        if parameter.binding == "path_variable" and parameter.description is not None
    }
    for argument_name, _ in path_argument_names:
        description = path_argument_docs.get(argument_name)
        if description is not None:
            arg_lines.append(f"{argument_name}: {description}")
    if request_params_arg_name is not None:
        payload_kind = (
            "Query parameters"
            if operation.http_method in {"DELETE", "GET"}
            else "Form parameters"
        )
        arg_lines.append(
            f"{request_params_arg_name}: {payload_kind} bag for this operation."
        )
    if has_model_attributes:
        arg_lines.append("request: Request payload.")
    if has_request_bodies:
        request_body = next(
            parameter
            for parameter in operation.parameters
            if parameter.binding == "request_body"
        )
        arg_lines.append(f"{snake_case(request_body.name)}: Request body payload.")
    if arg_lines:
        sections.append("Args:\n" + "\n".join(f"    {line}" for line in arg_lines))

    if operation.returns_doc is not None:
        sections.append(f"Returns:\n    {operation.returns_doc}")

    return "\n\n".join(section for section in sections if section)


def operation_response_use(
    operation: OperationSpec,
    context: PackageRenderContext,
) -> OperationResponseUse:
    operation_scope = ResolutionScope("operation_response", operation.operation_id)
    base_name = generic_base_type(operation.logical_return_type)
    if context.type_resolver.state(base_name) == "missing":
        return OperationResponseUse(
            operation.logical_return_type,
            operation_scope.owner_kind,
            operation_scope.owner_ref,
        )
    import_path = context.type_resolver.resolve(
        base_name,
        scope=operation_scope,
    )
    model = next(
        (
            candidate
            for candidate in context.snapshot.models
            if candidate.import_path == import_path
        ),
        None,
    )
    if model is None or model.kind != "generated_view" or len(model.fields) != 1:
        return OperationResponseUse(
            operation.logical_return_type,
            operation_scope.owner_kind,
            operation_scope.owner_ref,
        )
    field = model.fields[0]
    if field.name not in {"data", "dataList"}:
        return OperationResponseUse(
            operation.logical_return_type,
            operation_scope.owner_kind,
            operation_scope.owner_ref,
        )
    return OperationResponseUse(
        field.java_type,
        "structured_type",
        model.import_path,
    )


def _is_binary_response_operation(operation: OperationSpec) -> bool:
    """Identify the exact raw-download shapes used by DS REST controllers."""
    return not has_executable_response_type(operation)


def _is_void_response_operation(operation: OperationSpec) -> bool:
    return generic_base_type(operation.logical_return_type).rsplit(".", 1)[-1] in {
        "Void",
        "void",
    }
