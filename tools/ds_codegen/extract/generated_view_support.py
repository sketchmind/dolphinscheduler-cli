"""Generated-view registration and constant lookup helpers."""

from __future__ import annotations

import re
from dataclasses import replace
from typing import TYPE_CHECKING

import javalang

from ds_codegen.extract.metadata import _decode_scalar
from ds_codegen.extract.type_lookup import (
    _load_cached_type_declaration,
    _render_reference_name,
)
from ds_codegen.ir import DtoFieldSpec, ModelSpec
from ds_codegen.java_source import (
    SourceResolutionScope,
    qualify_java_type_references,
    resolve_referenced_import_path,
)
from ds_codegen.java_source import (
    load_primary_type_declaration_from_path as _load_primary_type_declaration_from_path,
)

if TYPE_CHECKING:
    from pathlib import Path

    from ds_codegen.ir import OperationSpec


_PAGE_INFO_IMPORT = "org.apache.dolphinscheduler.api.utils.PageInfo"


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
    owner_import_path: str | None,
    field_name: str,
    active_import_paths: tuple[str, ...] = (),
) -> javalang.tree.FieldDeclaration | None:
    context = find_field_context_in_hierarchy(
        repo_root=repo_root,
        type_declaration=type_declaration,
        import_map=import_map,
        package_name=package_name,
        owner_import_path=owner_import_path,
        field_name=field_name,
        active_import_paths=active_import_paths,
    )
    return context[0] if context is not None else None


def find_field_context_in_hierarchy(
    *,
    repo_root: Path,
    type_declaration: javalang.tree.TypeDeclaration,
    import_map: dict[str, str],
    package_name: str | None,
    owner_import_path: str | None,
    field_name: str,
    active_import_paths: tuple[str, ...] = (),
) -> (
    tuple[
        javalang.tree.FieldDeclaration,
        javalang.tree.TypeDeclaration,
        dict[str, str],
        str | None,
    ]
    | None
):
    """Return a field together with the Java context that declares it."""
    direct_field = find_field_declaration(type_declaration, field_name)
    if direct_field is not None:
        return direct_field, type_declaration, import_map, package_name
    if not isinstance(type_declaration, javalang.tree.ClassDeclaration):
        return None
    if type_declaration.extends is None:
        return None
    parent_import_path = resolve_referenced_import_path(
        repo_root,
        _render_reference_name(type_declaration.extends),
        SourceResolutionScope(import_map, package_name, owner_import_path),
    )
    if parent_import_path is None or parent_import_path in active_import_paths:
        return None
    loaded_parent_type = _load_cached_type_declaration(repo_root, parent_import_path)
    if loaded_parent_type is None:
        return None
    _, parent_type_declaration, parent_import_map, parent_package_name = (
        loaded_parent_type
    )
    return find_field_context_in_hierarchy(
        repo_root=repo_root,
        type_declaration=parent_type_declaration,
        import_map=parent_import_map,
        package_name=parent_package_name,
        owner_import_path=parent_import_path,
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
    active_constants: tuple[str, ...] = (),
) -> str | None:
    if isinstance(expression, javalang.tree.Literal):
        decoded_literal = _decode_scalar(expression)
        return decoded_literal if isinstance(decoded_literal, str) else None
    if isinstance(expression, javalang.tree.MemberReference):
        if expression.qualifier:
            owner_import_path = resolve_referenced_import_path(
                repo_root,
                expression.qualifier,
                SourceResolutionScope(import_map, package_name),
            )
            if owner_import_path is None:
                return None
            return load_static_string_constant_value(
                repo_root=repo_root,
                constant_import_path=owner_import_path,
                constant_name=expression.member,
                active_constants=active_constants,
            )
        resolved_owner_type = owner_type_declaration
        if resolved_owner_type is None and controller_path is not None:
            loaded_owner_type = _load_primary_type_declaration_from_path(
                controller_path
            )
            if loaded_owner_type is not None:
                _, resolved_owner_type, _, _ = loaded_owner_type
        if resolved_owner_type is not None:
            owner_context = find_field_context_in_hierarchy(
                repo_root=repo_root,
                type_declaration=resolved_owner_type,
                import_map=import_map,
                package_name=package_name,
                owner_import_path=(
                    f"{package_name}.{controller_path.stem}"
                    if package_name is not None and controller_path is not None
                    else None
                ),
                field_name=expression.member,
            )
            if owner_context is not None:
                owner_field, field_owner, field_import_map, field_package_name = (
                    owner_context
                )
                owner_identity = _constant_identity(
                    field_owner,
                    field_package_name,
                    expression.member,
                )
                if owner_identity in active_constants:
                    return None
                initializer = owner_field.declarators[0].initializer
                if initializer is None:
                    return None
                return resolve_string_constant_value(
                    repo_root=repo_root,
                    expression=initializer,
                    import_map=field_import_map,
                    package_name=field_package_name,
                    owner_type_declaration=field_owner,
                    active_constants=(*active_constants, owner_identity),
                )
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
            active_constants=active_constants,
        )
    return None


def load_static_string_constant_value(
    *,
    repo_root: Path,
    constant_import_path: str,
    constant_name: str,
    active_constants: tuple[str, ...] = (),
) -> str | None:
    loaded_constant_type = _load_cached_type_declaration(
        repo_root,
        constant_import_path,
    )
    if loaded_constant_type is None:
        return None
    _, type_declaration, import_map, package_name = loaded_constant_type
    constant_field = find_field_declaration(type_declaration, constant_name)
    if constant_field is None:
        return None
    constant_identity = _constant_identity(
        type_declaration,
        package_name,
        constant_name,
    )
    if constant_identity in active_constants:
        return None
    initializer = constant_field.declarators[0].initializer
    if initializer is None:
        return None
    return resolve_string_constant_value(
        repo_root=repo_root,
        expression=initializer,
        import_map=import_map,
        package_name=package_name,
        owner_type_declaration=type_declaration,
        active_constants=(*active_constants, constant_identity),
    )


def _constant_identity(
    owner_type: javalang.tree.TypeDeclaration,
    package_name: str | None,
    constant_name: str,
) -> str:
    owner_name = str(owner_type.name)
    qualified_owner = f"{package_name}.{owner_name}" if package_name else owner_name
    return f"{qualified_owner}.{constant_name}"


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
    repo_root: Path,
    generated_view_models: dict[str, ModelSpec],
    base_name: str,
    fields: list[tuple[str, str]],
    source_import_map: dict[str, str],
    source_package_name: str | None,
    source_owner_import_path: str | None,
) -> str:
    normalized_base_name = re.sub(r"[^0-9A-Za-z_]+", "_", base_name).strip("_")
    if not normalized_base_name:
        normalized_base_name = "GeneratedView"
    model_fields = [
        DtoFieldSpec(
            name=field_name,
            java_type=qualify_java_type_references(
                repo_root,
                java_type,
                SourceResolutionScope(
                    source_import_map,
                    source_package_name,
                    source_owner_import_path,
                ),
            ),
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
            model = ModelSpec(
                name=candidate_name,
                import_path=f"generated.view.{candidate_name}",
                kind="generated_view",
                documentation=None,
                extends=None,
                fields=model_fields,
            )
            generated_view_models[candidate_name] = model
            return candidate_name
        if existing_model.fields == model_fields:
            return candidate_name
        candidate_name = f"{normalized_base_name}_{suffix_index}"
        suffix_index += 1


def apply_reviewed_response_model_projections(
    *,
    operations: list[OperationSpec],
    source_models: list[ModelSpec],
    generated_view_models: dict[str, ModelSpec],
    operation_projections: dict[str, str],
) -> list[OperationSpec]:
    """Project source-reviewed operation closures without mutating source models."""
    if not operation_projections:
        return operations
    source_models_by_import = {model.import_path: model for model in source_models}
    projected_operations: list[OperationSpec] = []
    for operation in operations:
        projection = operation_projections.get(operation.operation_id)
        if projection is None:
            projected_operations.append(operation)
            continue
        if projection != "page_total_list_nullable_required":
            message = f"unknown response model projection {projection!r}"
            raise RuntimeError(message)
        source_model = source_models_by_import.get(_PAGE_INFO_IMPORT)
        if source_model is None:
            message = (
                f"{operation.operation_id} projection requires {_PAGE_INFO_IMPORT}"
            )
            raise RuntimeError(message)
        total_lists = [
            field for field in source_model.fields if field.wire_name == "totalList"
        ]
        if len(total_lists) != 1:
            message = (
                f"{operation.operation_id} projection requires one PageInfo.totalList"
            )
            raise RuntimeError(message)
        total_list = total_lists[0]
        if (
            total_list.java_type != "List<T>"
            or total_list.nullable
            or total_list.default_factory != "list"
        ):
            message = (
                f"{operation.operation_id} projection no longer matches the "
                "source PageInfo.totalList initializer"
            )
            raise RuntimeError(message)
        view_name = re.sub(
            r"[^0-9A-Za-z_]+",
            "_",
            f"{operation.operation_id}_{projection}",
        ).strip("_")
        view = replace(
            source_model,
            name=view_name,
            import_path=f"generated.view.{view_name}",
            kind="generated_view",
            extends=source_model.import_path,
            fields=[
                replace(
                    field,
                    java_type="Optional<List<T>>",
                    required=True,
                    nullable=False,
                    default_value=None,
                    default_factory=None,
                )
                if field.wire_name == "totalList"
                else field
                for field in source_model.fields
            ],
        )
        existing = generated_view_models.setdefault(view_name, view)
        if existing != view:
            message = f"conflicting generated response view {view_name!r}"
            raise RuntimeError(message)
        source_prefix = f"{_PAGE_INFO_IMPORT}<"
        if not operation.logical_return_type.startswith(source_prefix):
            message = (
                f"{operation.operation_id} projection expected PageInfo logical root"
            )
            raise RuntimeError(message)
        projected_operations.append(
            replace(
                operation,
                logical_return_type=(
                    f"{view.name}<{operation.logical_return_type[len(source_prefix) :]}"
                ),
            )
        )
    return projected_operations


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
