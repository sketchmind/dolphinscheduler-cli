"""Shared type and import resolution helpers for generated packages."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from ds_codegen.java_source import (
    load_type_declaration,
    resolve_referenced_import_path,
)
from ds_codegen.render.package.planner import python_class_name
from ds_codegen.render.requests_example import (
    _generic_base_type,
    _generic_inner_types,
    _split_generic_pair,
)

if TYPE_CHECKING:
    from ds_codegen.render.package.planner import PackageRenderContext

SCALAR_PYTHON_TYPES = {
    "Any": "object",
    "ArrayNode": "list[dict[str, object]]",
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


def field_annotation_type(
    java_type: str,
    *,
    allow_none: bool,
    owner_import_path: str | None,
    context: PackageRenderContext,
) -> str:
    rendered_type = render_annotation_type(
        java_type,
        owner_import_path=owner_import_path,
        context=context,
    )
    if not allow_none or rendered_type == "None" or rendered_type.endswith(" | None"):
        return rendered_type
    return f"{rendered_type} | None"


def render_annotation_type(
    java_type: str,
    *,
    owner_import_path: str | None,
    context: PackageRenderContext,
) -> str:
    if _is_type_variable(java_type):
        return java_type
    if java_type in context.specialized_by_java_type:
        return context.specialized_by_java_type[java_type].class_name
    if java_type in {"Byte[]", "byte[]"}:
        return "bytes"
    if java_type in SCALAR_PYTHON_TYPES:
        return SCALAR_PYTHON_TYPES[java_type]
    if java_type.endswith("[]"):
        inner_type = render_annotation_type(
            java_type[:-2],
            owner_import_path=owner_import_path,
            context=context,
        )
        return f"list[{inner_type}]"
    if java_type.startswith("Optional<") and java_type.endswith(">"):
        inner_type = render_annotation_type(
            java_type[9:-1],
            owner_import_path=owner_import_path,
            context=context,
        )
        return f"{inner_type} | None"
    if java_type.startswith("List<") and java_type.endswith(">"):
        inner_type = render_annotation_type(
            java_type[5:-1],
            owner_import_path=owner_import_path,
            context=context,
        )
        return f"list[{inner_type}]"
    if java_type.startswith("Set<") and java_type.endswith(">"):
        inner_type = render_annotation_type(
            java_type[4:-1],
            owner_import_path=owner_import_path,
            context=context,
        )
        return f"list[{inner_type}]"
    if java_type.startswith("Collection<") and java_type.endswith(">"):
        inner_type = render_annotation_type(
            java_type[11:-1],
            owner_import_path=owner_import_path,
            context=context,
        )
        return f"list[{inner_type}]"
    if java_type.startswith("Map<") and java_type.endswith(">"):
        key_type, value_type = _split_generic_pair(java_type[4:-1])
        return (
            "dict["
            + render_annotation_type(
                key_type, owner_import_path=owner_import_path, context=context
            )
            + ", "
            + render_annotation_type(
                value_type, owner_import_path=owner_import_path, context=context
            )
            + "]"
        )
    generic_base = _generic_base_type(java_type)
    generic_args = _generic_inner_types(java_type)
    if generic_args and java_type in context.specialized_by_java_type:
        return context.specialized_by_java_type[java_type].class_name
    import_path = resolve_owner_reference_import_path(
        generic_base,
        owner_import_path,
        context,
    )
    if import_path is not None and import_path in context.assignments_by_import_path:
        return context.assignments_by_import_path[import_path].class_name
    return python_class_name(generic_base)


def collect_annotation_import_targets(
    java_type: str,
    *,
    owner_import_path: str | None,
    current_module_parts: tuple[str, ...],
    context: PackageRenderContext,
) -> set[tuple[tuple[str, ...], str]]:
    targets: set[tuple[tuple[str, ...], str]] = set()
    if _is_type_variable(java_type):
        return targets
    if java_type in context.specialized_by_java_type:
        specialized = context.specialized_by_java_type[java_type]
        if specialized.module_parts != current_module_parts:
            targets.add((specialized.module_parts, specialized.class_name))
        return targets
    if java_type in {"Byte[]", "byte[]"} or java_type in SCALAR_PYTHON_TYPES:
        return targets
    if java_type.endswith("[]"):
        return collect_annotation_import_targets(
            java_type[:-2],
            owner_import_path=owner_import_path,
            current_module_parts=current_module_parts,
            context=context,
        )
    for prefix, offset in (
        ("Optional<", 9),
        ("List<", 5),
        ("Set<", 4),
        ("Collection<", 11),
    ):
        if java_type.startswith(prefix) and java_type.endswith(">"):
            return collect_annotation_import_targets(
                java_type[offset:-1],
                owner_import_path=owner_import_path,
                current_module_parts=current_module_parts,
                context=context,
            )
    if java_type.startswith("Map<") and java_type.endswith(">"):
        key_type, value_type = _split_generic_pair(java_type[4:-1])
        targets.update(
            collect_annotation_import_targets(
                key_type,
                owner_import_path=owner_import_path,
                current_module_parts=current_module_parts,
                context=context,
            )
        )
        targets.update(
            collect_annotation_import_targets(
                value_type,
                owner_import_path=owner_import_path,
                current_module_parts=current_module_parts,
                context=context,
            )
        )
        return targets
    generic_base = _generic_base_type(java_type)
    for generic_arg in _generic_inner_types(java_type):
        targets.update(
            collect_annotation_import_targets(
                generic_arg,
                owner_import_path=owner_import_path,
                current_module_parts=current_module_parts,
                context=context,
            )
        )
    import_path = resolve_owner_reference_import_path(
        generic_base,
        owner_import_path,
        context,
    )
    if import_path is None:
        return targets
    assignment = context.assignments_by_import_path.get(import_path)
    if assignment is None or assignment.module_parts == current_module_parts:
        return targets
    targets.add((assignment.module_parts, assignment.class_name))
    return targets


def resolve_owner_reference_import_path(
    reference_name: str,
    owner_import_path: str | None,
    context: PackageRenderContext,
) -> str | None:
    candidates = context.import_paths_by_name.get(reference_name, set())
    if len(candidates) == 1:
        return next(iter(candidates))
    if owner_import_path is not None and owner_import_path.startswith(
        ("org.apache.dolphinscheduler.", "com.")
    ):
        loaded = load_type_declaration(
            context.repo_root,
            owner_import_path,
            context.parse_cache,
        )
        if loaded is not None:
            _, _, import_map, package_name = loaded
            resolved = resolve_referenced_import_path(
                context.repo_root,
                reference_name,
                import_map,
                package_name,
                owner_import_path=owner_import_path,
            )
            if resolved is not None:
                return resolved
    if candidates:
        return sorted(
            candidates,
            key=lambda item: (not item.startswith("org.apache.dolphinscheduler"), item),
        )[0]
    return None


def relative_import_statement(
    from_module_parts: tuple[str, ...],
    to_module_parts: tuple[str, ...],
    class_name: str,
) -> str:
    from_package_parts = from_module_parts[:-1]
    to_package_parts = to_module_parts[:-1]
    common_length = 0
    for left, right in zip(from_package_parts, to_package_parts, strict=False):
        if left != right:
            break
        common_length += 1
    up_levels = len(from_package_parts) - common_length
    relative_prefix = "." * (up_levels + 1)
    target_suffix = ".".join(to_module_parts[common_length:])
    return f"from {relative_prefix}{target_suffix} import {class_name}"


def render_scalar_annotation_type(java_type: str) -> str:
    return SCALAR_PYTHON_TYPES.get(java_type, python_class_name(java_type))


def generic_substitutions(java_type: str) -> dict[str, str]:
    generic_args = _generic_inner_types(java_type)
    if len(generic_args) == 1:
        return {"T": generic_args[0]}
    return {}


def substitute_type_parameters(java_type: str, substitutions: dict[str, str]) -> str:
    if java_type in substitutions:
        return substitutions[java_type]
    if java_type.endswith("[]"):
        return substitute_type_parameters(java_type[:-2], substitutions) + "[]"
    if "<" not in java_type or not java_type.endswith(">"):
        return substitutions.get(java_type, java_type)
    generic_base = _generic_base_type(java_type)
    rendered_args = ", ".join(
        substitute_type_parameters(generic_arg, substitutions)
        for generic_arg in _generic_inner_types(java_type)
    )
    return f"{generic_base}<{rendered_args}>"


def _is_type_variable(java_type: str) -> bool:
    return bool(re.fullmatch(r"[A-Z]", java_type))
