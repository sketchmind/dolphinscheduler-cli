"""Render generated enum/model/dto modules from planned package context."""

from __future__ import annotations

import keyword
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar

from ds_codegen.contract_type_refs import (
    generic_base_type,
    generic_inner_types,
    substitute_type_parameters,
)
from ds_codegen.ir import (
    DtoFieldSpec,
    DtoSpec,
    EnumFieldSpec,
    EnumSpec,
    EnumValueSpec,
    ModelSpec,
)
from ds_codegen.java_literals import parse_java_numeric_literal
from ds_codegen.render.package.planner import python_class_name
from ds_codegen.render.package.render_support import (
    display_doc_text,
    pydantic_field_name,
    render_docstring_lines,
    render_field_default_expression,
    render_pydantic_field_config,
    wrap_comment_lines,
)
from ds_codegen.render.package.type_support import (
    collect_annotation_import_targets,
    field_annotation_type,
    generic_substitutions,
    relative_import_statement,
    render_annotation_type,
    render_scalar_annotation_type,
    resolve_owner_reference_import_path,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from ds_codegen.render.package.planner import PackageRenderContext, SpecializedModel

_RenderableSpec = EnumSpec | DtoSpec | ModelSpec
_StructuredSpec = TypeVar("_StructuredSpec", DtoSpec, ModelSpec)
_BASE_DATASOURCE_PARAM_DTO_IMPORT = (
    "org.apache.dolphinscheduler.plugin.datasource.api.datasource."
    "BaseDataSourceParamDTO"
)
_ROOT_MODEL_ALIAS_NAMES = {"JsonObject", "JsonValue"}
_NULLABLE_UPSTREAM_TASK_NAMES = frozenset(
    [
        *(f"3.0.{patch}" for patch in range(7)),
        *(f"3.1.{patch}" for patch in range(10)),
        *(f"3.2.{patch}" for patch in range(3)),
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
    ]
)
_TASK_MAIN_INFO = "org.apache.dolphinscheduler.dao.entity.TaskMainInfo"


@dataclass(frozen=True)
class TypeRenderDeps:
    base_contract_model_name: str
    base_view_model_name: str
    base_entity_model_name: str
    root_model_module_parts: tuple[str, ...]
    strict_integer_fields: Callable[[str], Mapping[str, tuple[str, ...]]]
    model_role_module_parts: Mapping[str, tuple[str, ...]]


def module_export_names(
    specs: list[_RenderableSpec],
    specialized_models: list[SpecializedModel],
    context: PackageRenderContext,
) -> list[str]:
    exported_names = [
        context.assignments_by_import_path[spec.import_path].class_name
        for spec in specs
    ]
    exported_names.extend(
        specialized.class_name
        for specialized in sorted(
            specialized_models,
            key=lambda item: item.class_name,
        )
    )
    return sorted(exported_names)


def _sort_structured_specs_by_inheritance(
    specs: list[_StructuredSpec],
    context: PackageRenderContext,
) -> list[_StructuredSpec]:
    """Place materialized parents before children inside a flattened module."""
    remaining = {spec.import_path: spec for spec in specs}
    ordered: list[_StructuredSpec] = []
    while remaining:
        ready: list[_StructuredSpec] = []
        for spec in remaining.values():
            parent_import_path = (
                None
                if spec.extends is None
                else resolve_owner_reference_import_path(
                    spec.extends,
                    spec.import_path,
                    context,
                )
            )
            if parent_import_path not in remaining:
                ready.append(spec)
        if not ready:
            message = (
                "generated structured-type inheritance contains a cycle: "
                f"{sorted(remaining)!r}"
            )
            raise ValueError(message)
        ready.sort(
            key=lambda item: (
                context.assignments_by_import_path[item.import_path].class_name
            )
        )
        ordered.extend(ready)
        for spec in ready:
            del remaining[spec.import_path]
    return ordered


def render_type_module(
    *,
    module_parts: tuple[str, ...],
    specs: list[_RenderableSpec],
    specialized_models: list[SpecializedModel],
    dto_specs_by_import_path: dict[str, DtoSpec],
    model_specs_by_import_path: dict[str, ModelSpec],
    context: PackageRenderContext,
    deps: TypeRenderDeps,
) -> str:
    enum_specs = sorted(
        [spec for spec in specs if isinstance(spec, EnumSpec)],
        key=lambda item: (
            context.assignments_by_import_path[item.import_path].class_name
        ),
    )
    dto_specs = _sort_structured_specs_by_inheritance(
        [spec for spec in specs if isinstance(spec, DtoSpec)],
        context,
    )
    model_specs = _sort_structured_specs_by_inheritance(
        [spec for spec in specs if isinstance(spec, ModelSpec)],
        context,
    )
    resolved_model_shapes = {
        model_spec.import_path: _resolve_model_render_shape(
            model_spec,
            model_specs_by_import_path,
            context,
            deps=deps,
        )
        for model_spec in model_specs
    }
    resolved_dto_shapes = {
        dto_spec.import_path: _resolve_dto_render_shape(
            dto_spec,
            dto_specs_by_import_path,
            model_specs_by_import_path,
            context,
            deps=deps,
        )
        for dto_spec in dto_specs
    }
    module_type_vars: set[str] = set()
    for _, own_fields in resolved_model_shapes.values():
        module_type_vars.update(_collect_type_variables(own_fields))
    for _, own_fields in resolved_dto_shapes.values():
        module_type_vars.update(_collect_type_variables(own_fields))

    import_targets: set[tuple[tuple[str, ...], str]] = set()
    for dto_spec in dto_specs:
        if dto_spec.extends is None:
            continue
        parent_import_path = resolve_owner_reference_import_path(
            dto_spec.extends,
            dto_spec.import_path,
            context,
        )
        if parent_import_path is None:
            continue
        parent_assignment = context.assignments_by_import_path[parent_import_path]
        if parent_assignment.module_parts == module_parts:
            continue
        import_targets.add(
            (parent_assignment.module_parts, parent_assignment.class_name)
        )
    for model_spec in model_specs:
        if model_spec.extends is None:
            continue
        parent_import_path = resolve_owner_reference_import_path(
            model_spec.extends,
            model_spec.import_path,
            context,
        )
        if parent_import_path is None:
            continue
        parent_assignment = context.assignments_by_import_path[parent_import_path]
        if parent_assignment.module_parts == module_parts:
            continue
        import_targets.add(
            (parent_assignment.module_parts, parent_assignment.class_name)
        )
    for specialized in specialized_models:
        base_assignment = context.assignments_by_import_path[
            specialized.base_import_path
        ]
        if base_assignment.module_parts == module_parts:
            continue
        import_targets.add((base_assignment.module_parts, base_assignment.class_name))
    for dto_spec in dto_specs:
        for field in dto_spec.fields:
            import_targets.update(
                collect_annotation_import_targets(
                    field.java_type,
                    owner_import_path=dto_spec.import_path,
                    current_module_parts=module_parts,
                    context=context,
                )
            )
    for model_spec in model_specs:
        for field in model_spec.fields:
            import_targets.update(
                collect_annotation_import_targets(
                    field.java_type,
                    owner_import_path=model_spec.import_path,
                    current_module_parts=module_parts,
                    context=context,
                )
            )
    for specialized in specialized_models:
        base_model = model_specs_by_import_path[specialized.base_import_path]
        substitutions = generic_substitutions(specialized.java_type)
        for field in base_model.fields:
            import_targets.update(
                collect_annotation_import_targets(
                    substitute_type_parameters(field.java_type, substitutions),
                    owner_import_path=specialized.base_import_path,
                    current_module_parts=module_parts,
                    context=context,
                    specialized_java_type=specialized.java_type,
                )
            )

    runtime_imports = sorted(
        relative_import_statement(module_parts, target_module_parts, class_name)
        for target_module_parts, class_name in import_targets
    )

    sections = [
        "from __future__ import annotations",
        "",
    ]
    uses_models = any(
        own_fields for _, own_fields in resolved_model_shapes.values()
    ) or any(own_fields for _, own_fields in resolved_dto_shapes.values())
    uses_models = uses_models or any(
        not _collect_type_variables(
            model_specs_by_import_path[specialized.base_import_path].fields
        )
        for specialized in specialized_models
    )
    base_model_names: set[str] = set()
    if dto_specs:
        base_model_names.update(
            primary_base_name
            for primary_base_name in (
                _primary_base_name(base_expr)
                for base_expr, _ in resolved_dto_shapes.values()
            )
            if primary_base_name
            in {
                deps.base_contract_model_name,
                deps.base_view_model_name,
                deps.base_entity_model_name,
            }
        )
        for dto_spec in dto_specs:
            base_model_names.update(
                _field_root_model_alias_names(
                    dto_spec.fields,
                    owner_import_path=dto_spec.import_path,
                    context=context,
                )
            )
    if model_specs:
        base_model_names.update(
            primary_base_name
            for primary_base_name in (
                _primary_base_name(base_expr)
                for base_expr, _ in resolved_model_shapes.values()
            )
            if primary_base_name
            in {
                deps.base_contract_model_name,
                deps.base_view_model_name,
                deps.base_entity_model_name,
            }
        )
        for model_spec in model_specs:
            base_model_names.update(
                _field_root_model_alias_names(
                    model_spec.fields,
                    owner_import_path=model_spec.import_path,
                    context=context,
                )
            )
    if specialized_models:
        base_model_names.update(
            _model_role_base_name(
                specialized.base_import_path,
                module_parts=specialized.module_parts,
                deps=deps,
            )
            for specialized in specialized_models
            if not _collect_type_variables(
                model_specs_by_import_path[specialized.base_import_path].fields
            )
        )
        for specialized in specialized_models:
            base_model = model_specs_by_import_path[specialized.base_import_path]
            substitutions = generic_substitutions(specialized.java_type)
            base_model_names.update(
                _field_root_model_alias_names(
                    [
                        DtoFieldSpec(
                            name=field.name,
                            java_type=substitute_type_parameters(
                                field.java_type,
                                substitutions,
                            ),
                            wire_name=field.wire_name,
                            required=field.required,
                            default_value=field.default_value,
                            nullable=field.nullable,
                            default_factory=field.default_factory,
                            description=field.description,
                            example=field.example,
                            allowable_values=field.allowable_values,
                            documentation=field.documentation,
                        )
                        for field in base_model.fields
                    ],
                    owner_import_path=specialized.base_import_path,
                    context=context,
                    specialized_java_type=specialized.java_type,
                )
            )
    if enum_specs:
        sections.append("from enum import Enum, IntEnum, StrEnum")
    if module_type_vars:
        sections.append("from typing import Generic, TypeVar")
    if uses_models:
        pydantic_import_names = ["Field"]
        if any(_needs_open_extra_model(model_spec) for model_spec in model_specs):
            pydantic_import_names.append("ConfigDict")
        model_uses_strict_integer = any(
            any(field.wire_name in strict_names for field in spec.fields)
            for spec in model_specs
            for strict_names in (
                _strict_integer_fields_for_model(spec, context=context, deps=deps).get(
                    spec.import_path, ()
                ),
            )
        )
        strict_integer_fields = deps.strict_integer_fields(context.snapshot.ds_version)
        dto_uses_strict_integer = any(
            field.wire_name in strict_integer_fields.get(spec.import_path, ())
            for spec in dto_specs
            for field in spec.fields
        )
        if model_uses_strict_integer or dto_uses_strict_integer:
            pydantic_import_names.append("StrictInt")
        sections.append(
            "from pydantic import " + ", ".join(sorted(pydantic_import_names))
        )
    if base_model_names:
        sections.append(
            relative_import_statement(
                module_parts,
                deps.root_model_module_parts,
                ", ".join(sorted(base_model_names)),
            )
        )
    if runtime_imports:
        sections.append("")
        sections.extend(sorted(dict.fromkeys(runtime_imports)))
    if module_type_vars:
        sections.append("")
        sections.extend(
            f'{type_var} = TypeVar("{type_var}")'
            for type_var in sorted(module_type_vars)
        )
    sections.append("")

    blocks: list[str] = [_render_package_enum(enum_spec) for enum_spec in enum_specs]
    blocks.extend(
        _render_model_class(
            model_spec,
            model_specs_by_import_path,
            context,
            deps=deps,
        )
        for model_spec in model_specs
    )
    blocks.extend(
        _render_dto_class(
            dto_spec,
            dto_specs_by_import_path,
            model_specs_by_import_path,
            context,
            deps=deps,
        )
        for dto_spec in dto_specs
    )
    blocks.extend(
        _render_specialized_model_class(
            specialized,
            model_specs_by_import_path[specialized.base_import_path],
            context,
            deps=deps,
        )
        for specialized in sorted(
            specialized_models,
            key=lambda item: item.class_name,
        )
    )
    sections.append("\n\n".join(blocks))
    exported_names = [
        context.assignments_by_import_path[spec.import_path].class_name
        for spec in enum_specs + model_specs + dto_specs
    ] + [specialized.class_name for specialized in specialized_models]
    sections.append("")
    sections.append(
        "__all__ = [" + ", ".join(f'"{name}"' for name in exported_names) + "]"
    )
    return "\n".join(section for section in sections if section is not None) + "\n"


def _enum_base_class(wire_type: str) -> str:
    if wire_type == "int":
        return "IntEnum"
    if wire_type == "str":
        return "StrEnum"
    return "Enum"


def _enum_wire_field(enum_spec: EnumSpec) -> EnumFieldSpec | None:
    if enum_spec.json_value_field is None:
        return None
    return next(
        (
            field
            for field in enum_spec.fields
            if field.name == enum_spec.json_value_field
        ),
        None,
    )


def _enum_member_attribute_name(field_name: str | None) -> str:
    if field_name is None:
        return "wire_value"
    if field_name in {"name", "value"}:
        return f"{field_name}_field"
    return field_name


def _enum_member_argument_name(field_name: str, attribute_name: str) -> str:
    argument_name = (
        f"{field_name}_arg" if field_name in {"name", "value"} else attribute_name
    )
    return f"{argument_name}_" if keyword.iskeyword(argument_name) else argument_name


def _render_enum_new_method(
    enum_spec: EnumSpec,
    enum_name: str,
    base_class: str,
    wire_type: str,
    member_fields: list[tuple[EnumFieldSpec, str]],
) -> list[str]:
    argument_specs: list[str] = []
    wire_field_name = enum_spec.json_value_field
    if wire_field_name is None:
        argument_specs.append(f"wire_value: {wire_type}")
    for field, attribute_name in member_fields:
        argument_name = _enum_member_argument_name(field.name, attribute_name)
        argument_specs.append(
            f"{argument_name}: {render_scalar_annotation_type(field.java_type)}"
        )

    lines = [f"    def __new__(cls, {', '.join(argument_specs)}) -> {enum_name}:"]
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
                render_scalar_annotation_type(field.java_type),
            )
        )
    if len(constructor_parts) == 1:
        return constructor_parts[0]
    return f"({', '.join(constructor_parts)})"


def _enum_supports_from_code(enum_spec: EnumSpec) -> bool:
    return any(
        field.name == "code" and render_scalar_annotation_type(field.java_type) == "int"
        for field in enum_spec.fields
    )


def _enum_wire_literal(
    enum_spec: EnumSpec,
    enum_value: EnumValueSpec,
    wire_type: str,
) -> str:
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
    return next(
        (
            index
            for index, field in enumerate(enum_spec.fields)
            if field.name == field_name
        ),
        None,
    )


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


def _render_enum_member_lines(
    enum_spec: EnumSpec,
    wire_type: str,
) -> list[str]:
    lines: list[str] = []
    for value in enum_spec.values:
        if value.documentation is not None:
            lines.extend(wrap_comment_lines(value.documentation, indent="    "))
        lines.append(
            f"    {value.name} = "
            f"{_render_enum_member_constructor(enum_spec, value, wire_type)}"
        )
    return lines


def _render_package_enum(enum_spec: EnumSpec) -> str:
    enum_name = python_class_name(enum_spec.name)
    wire_field = _enum_wire_field(enum_spec)
    wire_type = (
        render_scalar_annotation_type(wire_field.java_type)
        if wire_field is not None
        else "str"
    )
    base_class = _enum_base_class(wire_type)
    lines = [f"class {enum_name}({base_class}):"]
    if enum_spec.documentation is not None:
        lines.extend(
            render_docstring_lines(
                display_doc_text(enum_spec.documentation),
                indent="    ",
            )
        )
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
            f"    {attribute_name}: {render_scalar_annotation_type(field.java_type)}"
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
    lines.extend(_render_enum_member_lines(enum_spec, wire_type))
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


def _role_base_model_name(
    *,
    module_parts: tuple[str, ...],
    deps: TypeRenderDeps,
) -> str:
    if module_parts[:2] == ("api", "views"):
        return deps.base_view_model_name
    if module_parts[:2] == ("dao", "entities"):
        return deps.base_entity_model_name
    return deps.base_contract_model_name


def _model_role_base_name(
    import_path: str,
    *,
    module_parts: tuple[str, ...],
    deps: TypeRenderDeps,
) -> str:
    return _role_base_model_name(
        module_parts=deps.model_role_module_parts.get(import_path, module_parts),
        deps=deps,
    )


def _resolved_model_base_name(
    model_spec: ModelSpec,
    model_specs_by_import_path: dict[str, ModelSpec],
    context: PackageRenderContext,
    *,
    deps: TypeRenderDeps,
) -> str:
    assignment = context.assignments_by_import_path[model_spec.import_path]
    base_name = _model_role_base_name(
        model_spec.import_path,
        module_parts=assignment.module_parts,
        deps=deps,
    )
    if model_spec.extends is None:
        return base_name
    parent_import_path = resolve_owner_reference_import_path(
        model_spec.extends,
        model_spec.import_path,
        context,
    )
    if parent_import_path is None:
        return base_name
    parent_spec = model_specs_by_import_path.get(parent_import_path)
    if parent_spec is None:
        return base_name
    inherited_fields = _split_inherited_fields(
        model_spec.fields,
        parent_spec.fields,
    )
    if inherited_fields is None:
        return base_name
    return context.assignments_by_import_path[parent_import_path].class_name


def _resolved_dto_base_name(
    dto_spec: DtoSpec,
    dto_specs_by_import_path: dict[str, DtoSpec],
    model_specs_by_import_path: dict[str, ModelSpec],
    context: PackageRenderContext,
    *,
    deps: TypeRenderDeps,
) -> str:
    base_name = deps.base_contract_model_name
    if dto_spec.extends is None:
        return base_name
    parent_import_path = resolve_owner_reference_import_path(
        dto_spec.extends,
        dto_spec.import_path,
        context,
    )
    if parent_import_path is None:
        return base_name
    parent_dto_spec = dto_specs_by_import_path.get(parent_import_path)
    parent_model_spec = model_specs_by_import_path.get(parent_import_path)
    parent_fields = (
        parent_dto_spec.fields
        if parent_dto_spec is not None
        else parent_model_spec.fields
        if parent_model_spec is not None
        else None
    )
    if parent_fields is None:
        return base_name
    inherited_fields = _split_inherited_fields(
        dto_spec.fields,
        parent_fields,
    )
    if inherited_fields is None:
        return base_name
    return context.assignments_by_import_path[parent_import_path].class_name


def _render_model_class(
    model_spec: ModelSpec,
    model_specs_by_import_path: dict[str, ModelSpec],
    context: PackageRenderContext,
    *,
    deps: TypeRenderDeps,
) -> str:
    assignment = context.assignments_by_import_path[model_spec.import_path]
    class_name = assignment.class_name
    base_expr, own_fields = _resolve_model_render_shape(
        model_spec,
        model_specs_by_import_path,
        context,
        deps=deps,
    )
    type_vars = _collect_type_variables(own_fields)
    if type_vars:
        base_expr = _compose_generic_base_expression(base_expr, type_vars)
    return _render_pydantic_model_class(
        class_name,
        base_expr,
        own_fields,
        owner_import_path=model_spec.import_path,
        context=context,
        strict_integer_fields=_strict_integer_fields_for_model(
            model_spec, context=context, deps=deps
        ),
        extra_policy=("allow" if _needs_open_extra_model(model_spec) else None),
        docstring=(
            f"AST-inferred view from {model_spec.import_path}."
            if model_spec.import_path.startswith("generated.view.")
            else (
                display_doc_text(model_spec.documentation)
                if model_spec.documentation is not None
                else None
            )
        ),
    )


def _render_dto_class(
    dto_spec: DtoSpec,
    dto_specs_by_import_path: dict[str, DtoSpec],
    model_specs_by_import_path: dict[str, ModelSpec],
    context: PackageRenderContext,
    *,
    deps: TypeRenderDeps,
) -> str:
    assignment = context.assignments_by_import_path[dto_spec.import_path]
    class_name = assignment.class_name
    base_expr, own_fields = _resolve_dto_render_shape(
        dto_spec,
        dto_specs_by_import_path,
        model_specs_by_import_path,
        context,
        deps=deps,
    )
    type_vars = _collect_type_variables(own_fields)
    if type_vars:
        base_expr = _compose_generic_base_expression(base_expr, type_vars)
    return _render_pydantic_model_class(
        class_name,
        base_expr,
        own_fields,
        owner_import_path=dto_spec.import_path,
        context=context,
        strict_integer_fields=deps.strict_integer_fields(context.snapshot.ds_version),
        dto_required=True,
        docstring=(
            display_doc_text(dto_spec.documentation)
            if dto_spec.documentation is not None
            else None
        ),
    )


def _render_specialized_model_class(
    specialized: SpecializedModel,
    base_model: ModelSpec,
    context: PackageRenderContext,
    *,
    deps: TypeRenderDeps,
) -> str:
    base_class_name = context.assignments_by_import_path[
        specialized.base_import_path
    ].class_name
    base_type_vars = _collect_type_variables(base_model.fields)
    if base_type_vars:
        base_expr = _render_parameterized_base_expression(
            base_class_name,
            specialized.java_type,
            owner_import_path=specialized.base_import_path,
            context=context,
            resolution_java_type=specialized.java_type,
        )
        own_fields: list[DtoFieldSpec] = []
    else:
        substitutions = generic_substitutions(specialized.java_type)
        own_fields = [
            DtoFieldSpec(
                name=field.name,
                java_type=substitute_type_parameters(field.java_type, substitutions),
                wire_name=field.wire_name,
                required=field.required,
                default_value=field.default_value,
                nullable=field.nullable,
                default_factory=field.default_factory,
                description=field.description,
                example=field.example,
                allowable_values=field.allowable_values,
                documentation=field.documentation,
            )
            for field in base_model.fields
        ]
        base_expr = _model_role_base_name(
            specialized.base_import_path,
            module_parts=specialized.module_parts,
            deps=deps,
        )
    return _render_pydantic_model_class(
        specialized.class_name,
        base_expr,
        own_fields,
        owner_import_path=specialized.base_import_path,
        context=context,
        specialized_java_type=specialized.java_type,
        strict_integer_fields=_strict_integer_fields_for_model(
            base_model, context=context, deps=deps
        ),
        docstring=(f"Specialized view for {specialized.java_type}."),
    )


def _render_pydantic_model_class(
    class_name: str,
    base_expr: str,
    fields: list[DtoFieldSpec],
    *,
    owner_import_path: str,
    context: PackageRenderContext,
    strict_integer_fields: Mapping[str, tuple[str, ...]],
    specialized_java_type: str | None = None,
    dto_required: bool = False,
    extra_policy: str | None = None,
    docstring: str | None = None,
) -> str:
    normalized_fields = _dedupe_wire_fields(fields)
    header = f"class {class_name}({base_expr}):"
    lines = [header]
    if docstring is not None:
        lines.append(f'    """{docstring}"""')
    if extra_policy is not None:
        lines.extend(
            [
                "    model_config = ConfigDict(",
                "        populate_by_name=True,",
                "        arbitrary_types_allowed=True,",
                f'        extra="{extra_policy}",',
                "    )",
            ]
        )
    if not normalized_fields:
        if docstring is not None:
            return "\n".join(lines)
        lines.append("    pass")
        return "\n".join(lines)
    for field in normalized_fields:
        attribute_name = pydantic_field_name(field.name)
        is_required = _rendered_field_is_required(field, dto_required=dto_required)
        rendered_type = field_annotation_type(
            field.java_type,
            allow_none=field.nullable and not is_required,
            owner_import_path=owner_import_path,
            context=context,
            specialized_java_type=specialized_java_type,
        )
        rendered_type = _reviewed_map_value_nullability(
            version=context.snapshot.ds_version,
            owner_import_path=owner_import_path,
            field=field,
            rendered_type=rendered_type,
        )
        strict_field_names = strict_integer_fields.get(owner_import_path, ())
        if field.wire_name in strict_field_names:
            if rendered_type not in {"int", "int | None"}:
                message = (
                    "Cleanup strict integer overlay expected an integer field for "
                    f"{owner_import_path}.{field.wire_name}, got {rendered_type}"
                )
                raise ValueError(message)
            rendered_type = rendered_type.replace("int", "StrictInt", 1)
        default_expression = None
        default_factory = None
        if not is_required:
            default_expression = render_field_default_expression(field)
            default_factory = field.default_factory
        field_config = render_pydantic_field_config(
            field,
            required=is_required,
            attribute_name=attribute_name,
            default_expression=default_expression,
            default_factory=default_factory,
        )
        if field_config is None:
            if is_required:
                lines.append(f"    {attribute_name}: {rendered_type}")
            else:
                lines.append(f"    {attribute_name}: {rendered_type} = None")
        else:
            lines.append(
                f"    {attribute_name}: {rendered_type} = Field({field_config})"
            )
    return "\n".join(lines)


def _reviewed_map_value_nullability(
    *,
    version: str,
    owner_import_path: str,
    field: DtoFieldSpec,
    rendered_type: str,
) -> str:
    """Keep the upstream task-name map typed when its LEFT JOIN misses a row."""
    if (
        version not in _NULLABLE_UPSTREAM_TASK_NAMES
        or owner_import_path != _TASK_MAIN_INFO
        or field.wire_name != "upstreamTaskMap"
    ):
        return rendered_type
    # TaskDefinitionServiceImpl puts the LEFT JOIN's nullable upstream name
    # into this exact Java map without a null guard in these releases.
    if (field.java_type, field.nullable, rendered_type) != (
        "Map<Long, String>",
        True,
        "dict[int, str] | None",
    ):
        message = "reviewed TaskMainInfo.upstreamTaskMap shape changed"
        raise ValueError(message)
    return "dict[int, str | None] | None"


def _needs_open_extra_model(model_spec: ModelSpec) -> bool:
    return model_spec.import_path == _BASE_DATASOURCE_PARAM_DTO_IMPORT


def _dedupe_wire_fields(fields: list[DtoFieldSpec]) -> list[DtoFieldSpec]:
    last_field_by_wire_name: dict[str, DtoFieldSpec] = {}
    for field in fields:
        last_field_by_wire_name[field.wire_name] = field
    return [
        field for field in fields if last_field_by_wire_name[field.wire_name] is field
    ]


def _rendered_field_is_required(field: DtoFieldSpec, *, dto_required: bool) -> bool:
    if dto_required and field.required is True:
        return True
    if field.nullable:
        return False
    return field.default_value is None and field.default_factory is None


def _split_inherited_fields(
    child_fields: list[DtoFieldSpec],
    parent_fields: list[DtoFieldSpec],
) -> list[DtoFieldSpec] | None:
    if len(child_fields) < len(parent_fields):
        return None
    parent_keys = [field.wire_name for field in parent_fields]
    child_prefix_keys = [
        field.wire_name for field in child_fields[: len(parent_fields)]
    ]
    if child_prefix_keys != parent_keys:
        return None
    if child_fields[: len(parent_fields)] != parent_fields:
        return None
    own_fields = child_fields[len(parent_fields) :]
    if any(field.wire_name in set(parent_keys) for field in own_fields):
        return None
    return own_fields


def _strict_integer_fields_for_model(
    model_spec: ModelSpec,
    *,
    context: PackageRenderContext,
    deps: TypeRenderDeps,
) -> Mapping[str, tuple[str, ...]]:
    """Carry source-owner strict integer overlays into a derived view."""
    strict_fields = dict(deps.strict_integer_fields(context.snapshot.ds_version))
    if model_spec.kind != "generated_view" or model_spec.extends is None:
        return strict_fields
    source_import = resolve_owner_reference_import_path(
        model_spec.extends,
        model_spec.import_path,
        context,
    )
    if source_import is None or source_import not in strict_fields:
        return strict_fields
    strict_fields[model_spec.import_path] = strict_fields[source_import]
    return strict_fields


def _resolve_model_render_shape(
    model_spec: ModelSpec,
    model_specs_by_import_path: dict[str, ModelSpec],
    context: PackageRenderContext,
    *,
    deps: TypeRenderDeps,
) -> tuple[str, list[DtoFieldSpec]]:
    assignment = context.assignments_by_import_path[model_spec.import_path]
    role_base_name = _model_role_base_name(
        model_spec.import_path,
        module_parts=assignment.module_parts,
        deps=deps,
    )
    base_expr = _resolved_model_base_name(
        model_spec,
        model_specs_by_import_path,
        context,
        deps=deps,
    )
    own_fields = model_spec.fields
    if model_spec.extends is None:
        return base_expr, own_fields
    parent_import_path = resolve_owner_reference_import_path(
        model_spec.extends,
        model_spec.import_path,
        context,
    )
    if parent_import_path is None:
        return base_expr, own_fields
    parent_spec = model_specs_by_import_path.get(parent_import_path)
    if parent_spec is None:
        return base_expr, own_fields
    parent_class_name = context.assignments_by_import_path[
        parent_import_path
    ].class_name
    generic_parent = _resolve_specialized_parent_base_expression(
        child_fields=model_spec.fields,
        parent_fields=parent_spec.fields,
        parent_class_name=parent_class_name,
        owner_import_path=model_spec.import_path,
        context=context,
    )
    if generic_parent is not None:
        return generic_parent
    if base_expr == role_base_name:
        return base_expr, own_fields
    inherited_fields = _split_inherited_fields(
        model_spec.fields,
        parent_spec.fields,
    )
    if inherited_fields is None:
        return base_expr, own_fields
    raw_generic_parent = _default_generic_parent_base_expression(
        parent_class_name,
        parent_spec.fields,
    )
    if raw_generic_parent is not None:
        return raw_generic_parent, inherited_fields
    return parent_class_name, inherited_fields


def _resolve_dto_render_shape(
    dto_spec: DtoSpec,
    dto_specs_by_import_path: dict[str, DtoSpec],
    model_specs_by_import_path: dict[str, ModelSpec],
    context: PackageRenderContext,
    *,
    deps: TypeRenderDeps,
) -> tuple[str, list[DtoFieldSpec]]:
    base_expr = _resolved_dto_base_name(
        dto_spec,
        dto_specs_by_import_path,
        model_specs_by_import_path,
        context,
        deps=deps,
    )
    own_fields = dto_spec.fields
    if dto_spec.extends is None:
        return base_expr, own_fields
    parent_import_path = resolve_owner_reference_import_path(
        dto_spec.extends,
        dto_spec.import_path,
        context,
    )
    if parent_import_path is None:
        return base_expr, own_fields
    parent_dto_spec = dto_specs_by_import_path.get(parent_import_path)
    parent_model_spec = model_specs_by_import_path.get(parent_import_path)
    parent_fields = (
        parent_dto_spec.fields
        if parent_dto_spec is not None
        else parent_model_spec.fields
        if parent_model_spec is not None
        else None
    )
    if parent_fields is None:
        return base_expr, own_fields
    parent_class_name = context.assignments_by_import_path[
        parent_import_path
    ].class_name
    generic_parent = _resolve_specialized_parent_base_expression(
        child_fields=dto_spec.fields,
        parent_fields=parent_fields,
        parent_class_name=parent_class_name,
        owner_import_path=dto_spec.import_path,
        context=context,
    )
    if generic_parent is not None:
        return generic_parent
    if base_expr == deps.base_contract_model_name:
        return base_expr, own_fields
    inherited_fields = _split_inherited_fields(
        dto_spec.fields,
        parent_fields,
    )
    if inherited_fields is None:
        return base_expr, own_fields
    raw_generic_parent = _default_generic_parent_base_expression(
        parent_class_name,
        parent_fields,
    )
    if raw_generic_parent is not None:
        return raw_generic_parent, inherited_fields
    return parent_class_name, inherited_fields


def _resolve_specialized_parent_base_expression(
    *,
    child_fields: list[DtoFieldSpec],
    parent_fields: list[DtoFieldSpec],
    parent_class_name: str,
    owner_import_path: str,
    context: PackageRenderContext,
) -> tuple[str, list[DtoFieldSpec]] | None:
    parent_field_map = {field.wire_name: field for field in parent_fields}
    child_wire_names = {field.wire_name for field in child_fields}
    if not set(parent_field_map).issubset(child_wire_names):
        return None
    parent_type_vars = _collect_type_variables(parent_fields)
    if not parent_type_vars:
        return None

    substitutions: dict[str, str] = {}
    saw_specialization = False
    for child_field in child_fields:
        parent_field = parent_field_map.get(child_field.wire_name)
        if parent_field is None:
            continue
        field_substitutions = _infer_type_parameter_substitutions(
            parent_field.java_type,
            child_field.java_type,
        )
        if field_substitutions is None:
            if child_field.java_type == parent_field.java_type:
                continue
            return None
        if child_field.java_type != parent_field.java_type:
            saw_specialization = True
        if not _merge_type_parameter_substitutions(substitutions, field_substitutions):
            return None

    if not saw_specialization:
        return None

    rendered_args = ", ".join(
        render_annotation_type(
            substitutions.get(type_var, type_var),
            owner_import_path=owner_import_path,
            context=context,
        )
        for type_var in parent_type_vars
    )
    own_fields = [
        field for field in child_fields if field.wire_name not in parent_field_map
    ]
    return f"{parent_class_name}[{rendered_args}]", own_fields


def _infer_type_parameter_substitutions(
    parent_java_type: str,
    child_java_type: str,
) -> dict[str, str] | None:
    if parent_java_type == child_java_type:
        return {}
    if _is_type_variable(parent_java_type):
        return {parent_java_type: child_java_type}
    if parent_java_type.endswith("[]") and child_java_type.endswith("[]"):
        return _infer_type_parameter_substitutions(
            parent_java_type[:-2],
            child_java_type[:-2],
        )
    parent_base = _generic_base_name(parent_java_type)
    child_base = _generic_base_name(child_java_type)
    if parent_base is None or child_base is None or parent_base != child_base:
        return None
    parent_args = _generic_arguments(parent_java_type)
    child_args = _generic_arguments(child_java_type)
    if len(parent_args) != len(child_args):
        return None
    substitutions: dict[str, str] = {}
    for parent_arg, child_arg in zip(parent_args, child_args, strict=False):
        nested = _infer_type_parameter_substitutions(parent_arg, child_arg)
        if nested is None or not _merge_type_parameter_substitutions(
            substitutions,
            nested,
        ):
            return None
    return substitutions


def _merge_type_parameter_substitutions(
    target: dict[str, str],
    updates: dict[str, str],
) -> bool:
    for type_var, java_type in updates.items():
        existing = target.get(type_var)
        if existing is not None and existing != java_type:
            return False
        target[type_var] = java_type
    return True


def _collect_type_variables(fields: list[DtoFieldSpec]) -> tuple[str, ...]:
    ordered_type_vars: dict[str, None] = {}
    for field in fields:
        for type_var in _type_variables_in_java_type(field.java_type):
            ordered_type_vars.setdefault(type_var, None)
    return tuple(ordered_type_vars)


def _type_variables_in_java_type(java_type: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(re.findall(r"\b([A-Z])\b", java_type)))


def _compose_generic_base_expression(
    base_expr: str,
    type_vars: tuple[str, ...],
) -> str:
    rendered_type_vars = ", ".join(type_vars)
    return f"{base_expr}, Generic[{rendered_type_vars}]"


def _default_generic_parent_base_expression(
    parent_class_name: str,
    parent_fields: list[DtoFieldSpec],
) -> str | None:
    parent_type_vars = _collect_type_variables(parent_fields)
    if not parent_type_vars:
        return None
    return f"{parent_class_name}[{', '.join('object' for _ in parent_type_vars)}]"


def _primary_base_name(base_expr: str) -> str:
    return base_expr.split(", ", 1)[0]


def _field_root_model_alias_names(
    fields: list[DtoFieldSpec],
    *,
    owner_import_path: str,
    context: PackageRenderContext,
    specialized_java_type: str | None = None,
) -> set[str]:
    import_names: set[str] = set()
    for field in fields:
        rendered_annotation = render_annotation_type(
            field.java_type,
            owner_import_path=owner_import_path,
            context=context,
            specialized_java_type=specialized_java_type,
        )
        for alias_name in _ROOT_MODEL_ALIAS_NAMES:
            if re.search(rf"\b{re.escape(alias_name)}\b", rendered_annotation):
                import_names.add(alias_name)
    return import_names


def _render_parameterized_base_expression(
    base_class_name: str,
    specialized_java_type: str,
    *,
    owner_import_path: str,
    context: PackageRenderContext,
    resolution_java_type: str | None = None,
) -> str:
    rendered_args = ", ".join(
        render_annotation_type(
            generic_arg,
            owner_import_path=owner_import_path,
            context=context,
            specialized_java_type=resolution_java_type,
        )
        for generic_arg in _generic_arguments(specialized_java_type)
    )
    return f"{base_class_name}[{rendered_args}]"


def _generic_arguments(java_type: str) -> list[str]:
    if "<" not in java_type or not java_type.endswith(">"):
        return []
    return generic_inner_types(java_type)


def _generic_base_name(java_type: str) -> str | None:
    if "<" not in java_type or not java_type.endswith(">"):
        return None
    return generic_base_type(java_type)


def _is_type_variable(java_type: str) -> bool:
    return bool(re.fullmatch(r"[A-Z]", java_type))
