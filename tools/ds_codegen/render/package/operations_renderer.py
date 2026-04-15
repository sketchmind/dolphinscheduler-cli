"""Render generated operation modules with explicit helper dependencies."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ds_codegen.render.package.surface_renderer import (
    controller_module_name,
    controller_operations_class_name,
)
from ds_codegen.render.requests_client import (
    _explicit_content_type,
    _visible_operation_parameters,
    _visible_request_params,
)
from ds_codegen.render.requests_example import (
    _generic_base_type,
    _parameter_is_required,
    _render_path_arguments,
    _render_python_path_template,
    _RenderContext,
    _snake_case,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from ds_codegen.ir import OperationSpec
    from ds_codegen.render.package.planner import PackageRenderContext

AnnotationImportTarget = tuple[tuple[str, ...], str]


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
    base_import_names = [deps.requests_base_class_name]
    if uses_request_params:
        base_import_names.append(deps.base_params_model_name)
    if needs_upload_alias:
        base_import_names.append("UploadFileLike")
    base_import = f"from ._base import {', '.join(base_import_names)}"
    uses_validated_returns = any(
        deps.render_annotation_type(
            _inferred_return_type(operation, context),
            owner_import_path=None,
            context=context,
        )
        != "None"
        for operation in operations
    )

    sections = [
        "from __future__ import annotations",
        "",
        base_import,
        "",
    ]
    if uses_request_params and uses_validated_returns:
        sections.extend(["from pydantic import Field, TypeAdapter", ""])
    elif uses_request_params:
        sections.extend(["from pydantic import Field", ""])
    elif uses_validated_returns:
        sections.extend(["from pydantic import TypeAdapter", ""])

    import_targets: set[AnnotationImportTarget] = set()
    current_module_parts = ("api", "operations", module_name)
    for operation in operations:
        for parameter in _visible_request_params(operation):
            import_targets.update(
                deps.collect_annotation_import_targets(
                    parameter.java_type,
                    owner_import_path=None,
                    current_module_parts=current_module_parts,
                    context=context,
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
                    owner_import_path=None,
                    current_module_parts=current_module_parts,
                    context=context,
                )
            )
        import_targets.update(
            deps.collect_annotation_import_targets(
                _inferred_return_type(operation, context),
                owner_import_path=None,
                current_module_parts=current_module_parts,
                context=context,
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
        _render_operation_request_params(operation, context, deps=deps)
        for operation in operations
        if _visible_request_params(operation)
    ]
    if request_param_blocks:
        sections.append("\n\n".join(request_param_blocks))
        sections.append("")

    method_blocks = [
        _render_operation_method(operation, context, deps=deps)
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


def _render_operation_request_params(
    operation: OperationSpec,
    context: PackageRenderContext,
    *,
    deps: OperationRenderDeps,
) -> str:
    class_name = _operation_request_params_class_name(operation, deps=deps)
    lines = [f"class {class_name}({deps.base_params_model_name}):"]
    lines.extend(
        deps.render_docstring_lines(
            _operation_params_docstring(operation, deps=deps),
            indent="    ",
        )
    )
    for parameter in _visible_request_params(operation):
        is_required = _parameter_is_required(parameter)
        attribute_name = deps.pydantic_field_name(parameter.wire_name or parameter.name)
        rendered_type = deps.field_annotation_type(
            parameter.java_type,
            allow_none=not is_required,
            owner_import_path=None,
            context=context,
        )
        field_config = deps.render_parameter_field_config(
            parameter,
            required=is_required,
            attribute_name=attribute_name,
        )
        if field_config is None:
            if is_required:
                lines.append(f"    {attribute_name}: {rendered_type}")
            else:
                lines.append(f"    {attribute_name}: {rendered_type} = None")
            continue
        lines.append(f"    {attribute_name}: {rendered_type} = Field({field_config})")
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
    path_arguments = _render_path_arguments(operation, _empty_name_context(context))
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
                owner_import_path=None,
                context=context,
            )
        )
    if request_bodies:
        request_body = request_bodies[0]
        signature_items.append(
            f"{_snake_case(request_body.name)}: "
            + deps.render_annotation_type(
                request_body.java_type,
                owner_import_path=None,
                context=context,
            )
        )

    return_type = deps.render_annotation_type(
        _inferred_return_type(operation, context),
        owner_import_path=None,
        context=context,
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
        body_arg_name = _snake_case(request_body.name)
        if request_body.java_type == "String":
            if explicit_content_type is None:
                message = "String request bodies must render one explicit content type"
                raise ValueError(message)
            lines.append(
                f'        headers = {{"Content-Type": "{explicit_content_type}"}}'
            )
            request_keyword_lines.append(f"            content={body_arg_name},")
            request_keyword_lines.append("            headers=headers,")
        else:
            request_keyword_lines.append(
                f"            json=self._json_payload({body_arg_name}),"
            )
    elif explicit_content_type is not None:
        lines.append(f'        headers = {{"Content-Type": "{explicit_content_type}"}}')
        request_keyword_lines.append("            headers=headers,")

    if not request_keyword_lines:
        request_expr = f'self._request("{operation.http_method}", {request_path})'
        if return_type == "None":
            lines.append(f"        {request_expr}")
            lines.append("        return None")
        else:
            lines.append(f"        payload = {request_expr}")
            lines.extend(_projected_payload_lines(operation))
            lines.append(
                "        "
                f"return self._validate_payload(payload, TypeAdapter({return_type}))"
            )
        return "\n".join(lines)

    request_call_lines = [
        "self._request(",
        f'    "{operation.http_method}",',
        f"    {request_path},",
        *[line.strip() for line in request_keyword_lines],
        ")",
    ]
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
        f"        return self._validate_payload(payload, TypeAdapter({return_type}))"
    )
    return "\n".join(lines)


def _projected_payload_lines(operation: OperationSpec) -> list[str]:
    if operation.response_projection == "status_data":
        return ["        payload = self._project_status_data(payload)"]
    if operation.response_projection == "single_data":
        return ["        payload = self._project_single_data(payload)"]
    if operation.response_projection == "single_data_list":
        return ["        payload = self._project_single_data_list(payload)"]
    return []


def _operation_method_name(operation: OperationSpec) -> str:
    suffix = operation.operation_id.split(".", 1)[1]
    return _snake_case(suffix)


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
    detail_parts.append(
        f"DS operation: {operation.controller}.{operation.method_name} | "
        f"{operation.http_method} /{operation.path}"
    )
    sections.append("\n\n".join(detail_parts))

    arg_lines: list[str] = []
    path_argument_docs = {
        _snake_case(parameter.name): parameter.description
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
        arg_lines.append(f"{_snake_case(request_body.name)}: Request body payload.")
    if arg_lines:
        sections.append("Args:\n" + "\n".join(f"    {line}" for line in arg_lines))

    if operation.returns_doc is not None:
        sections.append(f"Returns:\n    {operation.returns_doc}")

    return "\n\n".join(section for section in sections if section)


def _inferred_return_type(
    operation: OperationSpec,
    context: PackageRenderContext,
) -> str:
    name_context = _empty_name_context(context)
    model = name_context.models_by_name.get(
        _generic_base_type(operation.logical_return_type)
    )
    if model is None or model.kind != "generated_view" or len(model.fields) != 1:
        return operation.logical_return_type
    field = model.fields[0]
    if field.name not in {"data", "dataList"}:
        return operation.logical_return_type
    return field.java_type


def _empty_name_context(context: PackageRenderContext) -> _RenderContext:
    return _RenderContext(
        dtos_by_name={dto.name: dto for dto in context.snapshot.dtos},
        models_by_name={model.name: model for model in context.snapshot.models},
        enums_by_name={
            enum_spec.name: enum_spec for enum_spec in context.snapshot.enums
        },
    )
