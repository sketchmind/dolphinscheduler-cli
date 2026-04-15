"""Generated-view registration and constant lookup helpers."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import javalang

from ds_codegen.extract.metadata import _decode_scalar
from ds_codegen.extract.type_lookup import (
    _load_cached_type_declaration,
    _render_reference_name,
)
from ds_codegen.ir import DtoFieldSpec, ModelSpec
from ds_codegen.java_source import (
    load_primary_type_declaration_from_path as _load_primary_type_declaration_from_path,
)
from ds_codegen.java_source import resolve_referenced_import_path

if TYPE_CHECKING:
    from pathlib import Path


def find_field_declaration(
    type_declaration: javalang.tree.TypeDeclaration,
    field_name: str,
) -> javalang.tree.FieldDeclaration | None:
    for field in getattr(type_declaration, "fields", []):
        for declarator in field.declarators:
            if declarator.name == field_name:
                return field
    return None


def find_field_declaration_in_hierarchy(
    *,
    repo_root: Path,
    type_declaration: javalang.tree.TypeDeclaration,
    import_map: dict[str, str],
    package_name: str | None,
    field_name: str,
    active_import_paths: tuple[str, ...] = (),
) -> javalang.tree.FieldDeclaration | None:
    direct_field = find_field_declaration(type_declaration, field_name)
    if direct_field is not None:
        return direct_field
    if not isinstance(type_declaration, javalang.tree.ClassDeclaration):
        return None
    if type_declaration.extends is None:
        return None
    parent_import_path = resolve_referenced_import_path(
        repo_root,
        _render_reference_name(type_declaration.extends),
        import_map,
        package_name,
    )
    if parent_import_path is None or parent_import_path in active_import_paths:
        return None
    loaded_parent_type = _load_cached_type_declaration(repo_root, parent_import_path)
    if loaded_parent_type is None:
        return None
    _, parent_type_declaration, parent_import_map, parent_package_name = (
        loaded_parent_type
    )
    return find_field_declaration_in_hierarchy(
        repo_root=repo_root,
        type_declaration=parent_type_declaration,
        import_map=parent_import_map,
        package_name=parent_package_name,
        field_name=field_name,
        active_import_paths=(*active_import_paths, parent_import_path),
    )


def resolve_string_constant_value(
    *,
    repo_root: Path,
    expression: object,
    import_map: dict[str, str],
    package_name: str | None,
    controller_path: Path | None = None,
    owner_type_declaration: javalang.tree.TypeDeclaration | None = None,
) -> str | None:
    if isinstance(expression, javalang.tree.Literal):
        decoded_literal = _decode_scalar(expression)
        return decoded_literal if isinstance(decoded_literal, str) else None
    if isinstance(expression, javalang.tree.MemberReference):
        if expression.qualifier:
            owner_import_path = resolve_referenced_import_path(
                repo_root,
                expression.qualifier,
                import_map,
                package_name,
            )
            if owner_import_path is None:
                return None
            return load_static_string_constant_value(
                repo_root=repo_root,
                constant_import_path=owner_import_path,
                constant_name=expression.member,
            )
        resolved_owner_type = owner_type_declaration
        if resolved_owner_type is None and controller_path is not None:
            loaded_owner_type = _load_primary_type_declaration_from_path(
                controller_path
            )
            if loaded_owner_type is not None:
                _, resolved_owner_type, _, _ = loaded_owner_type
        if resolved_owner_type is not None:
            owner_field = find_field_declaration_in_hierarchy(
                repo_root=repo_root,
                type_declaration=resolved_owner_type,
                import_map=import_map,
                package_name=package_name,
                field_name=expression.member,
            )
            if owner_field is not None:
                return decode_string_field_initializer(owner_field)
        static_import_path = import_map.get(f"@static:{expression.member}")
        if static_import_path is None:
            return None
        owner_import_path, _, constant_name = static_import_path.rpartition(".")
        if not owner_import_path or not constant_name:
            return None
        return load_static_string_constant_value(
            repo_root=repo_root,
            constant_import_path=owner_import_path,
            constant_name=constant_name,
        )
    return None


def load_static_string_constant_value(
    *,
    repo_root: Path,
    constant_import_path: str,
    constant_name: str,
) -> str | None:
    loaded_constant_type = _load_cached_type_declaration(
        repo_root,
        constant_import_path,
    )
    if loaded_constant_type is None:
        return None
    _, type_declaration, _, _ = loaded_constant_type
    constant_field = find_field_declaration(type_declaration, constant_name)
    if constant_field is None:
        return None
    return decode_string_field_initializer(constant_field)


def decode_string_field_initializer(
    field: javalang.tree.FieldDeclaration,
) -> str | None:
    declarator = field.declarators[0]
    if declarator.initializer is None:
        return None
    decoded_initializer = _decode_scalar(declarator.initializer)
    return decoded_initializer if isinstance(decoded_initializer, str) else None


def register_generated_view_model(
    *,
    generated_view_models: dict[str, ModelSpec],
    base_name: str,
    fields: list[tuple[str, str]],
) -> str:
    normalized_base_name = re.sub(r"[^0-9A-Za-z_]+", "_", base_name).strip("_")
    if not normalized_base_name:
        normalized_base_name = "GeneratedView"
    model_fields = [
        DtoFieldSpec(
            name=field_name,
            java_type=java_type,
            wire_name=field_name,
            required=None,
            default_value=None,
            nullable=True,
            default_factory=None,
            description=None,
            example=None,
            allowable_values=None,
            documentation=None,
        )
        for field_name, java_type in fields
    ]
    candidate_name = normalized_base_name
    suffix_index = 2
    while True:
        existing_model = generated_view_models.get(candidate_name)
        if existing_model is None:
            generated_view_models[candidate_name] = ModelSpec(
                name=candidate_name,
                import_path=f"generated.view.{candidate_name}",
                kind="generated_view",
                documentation=None,
                extends=None,
                fields=model_fields,
            )
            return candidate_name
        if existing_model.fields == model_fields:
            return candidate_name
        candidate_name = f"{normalized_base_name}_{suffix_index}"
        suffix_index += 1


def unwrap_generated_view_data_list_type(
    generated_view_models: dict[str, ModelSpec],
    java_type: str | None,
) -> str | None:
    if java_type is None:
        return None
    generated_model = generated_view_models.get(java_type)
    if generated_model is None:
        return None
    for field in generated_model.fields:
        if field.wire_name in {"data", "dataList"}:
            return field.java_type
    return None


def generated_view_model_fields(
    generated_view_models: dict[str, ModelSpec],
    model_name: str,
) -> list[tuple[str, str]] | None:
    generated_model = generated_view_models.get(model_name)
    if generated_model is None:
        return None
    return [(field.wire_name, field.java_type) for field in generated_model.fields]
