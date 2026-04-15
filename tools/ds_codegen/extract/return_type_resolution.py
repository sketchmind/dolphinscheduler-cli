"""Resolve operation-level logical payload types from raw controller signatures."""

from __future__ import annotations

from typing import TYPE_CHECKING

import javalang

from ds_codegen.extract.inference_support import _is_weak_inferred_type
from ds_codegen.extract.type_lookup import _render_reference_name, _render_type
from ds_codegen.java_source import load_type_declaration, resolve_referenced_import_path

if TYPE_CHECKING:
    from pathlib import Path

    from ds_codegen.ir import ResponseProjection


def unwrap_result_like_type(java_type: str) -> str | None:
    """Return the payload type for direct result-like wrappers."""

    if java_type == "Result":
        return "Object"
    if java_type.startswith("Result<") and java_type.endswith(">"):
        return java_type[7:-1]
    if java_type == "TaskInstanceSuccessResponse":
        return "Void"
    return None


def resolve_operation_logical_return_type(
    *,
    repo_root: Path,
    raw_return_type: str,
    inferred_return_type: str | None,
    import_map: dict[str, str],
    package_name: str | None,
) -> str:
    """Resolve the logical payload returned by an operation after DS envelope unwrap."""

    direct_payload_type = unwrap_result_like_type(raw_return_type)
    if inferred_return_type is not None and not _is_weak_logical_type_candidate(
        inferred_return_type
    ):
        return inferred_return_type
    if direct_payload_type is not None:
        if direct_payload_type != "Object":
            return direct_payload_type
        if inferred_return_type is not None:
            return inferred_return_type
        return direct_payload_type
    specialized_payload_type = _resolve_result_wrapper_payload_type(
        repo_root=repo_root,
        return_type=raw_return_type,
        import_map=import_map,
        package_name=package_name,
        active_return_types=(),
    )
    if specialized_payload_type is not None:
        if specialized_payload_type != "Object":
            return specialized_payload_type
        if inferred_return_type is not None:
            return inferred_return_type
        return specialized_payload_type
    if inferred_return_type is not None:
        return inferred_return_type
    return raw_return_type


def resolve_operation_response_projection(
    *,
    raw_return_type: str,
    logical_return_type: str,
) -> ResponseProjection:
    """Return the generated-client projection needed after transport unwrap."""
    if (
        raw_return_type
        in {
            "Map<String, Object>",
            "Map<String, Map<String, Object>>",
        }
        and logical_return_type != raw_return_type
    ):
        return "status_data"
    return "direct"


def _is_weak_logical_type_candidate(java_type: str) -> bool:
    return _is_weak_inferred_type(java_type) or java_type.endswith("_PageInfo_created")


def _resolve_result_wrapper_payload_type(
    *,
    repo_root: Path,
    return_type: str,
    import_map: dict[str, str],
    package_name: str | None,
    active_return_types: tuple[str, ...],
) -> str | None:
    base_return_type = _base_java_type(return_type)
    if base_return_type in active_return_types:
        return None
    return_import_path = resolve_referenced_import_path(
        repo_root,
        base_return_type,
        import_map,
        package_name,
    )
    if return_import_path is None:
        return None
    loaded_declaration = load_type_declaration(repo_root, return_import_path, {})
    if loaded_declaration is None:
        return None
    _, type_declaration, type_import_map, type_package_name = loaded_declaration
    if not isinstance(type_declaration, javalang.tree.ClassDeclaration):
        return None

    direct_data_type = _concrete_data_field_type(type_declaration)
    if direct_data_type is not None:
        return direct_data_type

    extends = type_declaration.extends
    if extends is None:
        return None
    extends_name = _render_reference_name(extends)
    direct_payload_type = unwrap_result_like_type(extends_name)
    if direct_payload_type is not None:
        return direct_payload_type
    return _resolve_result_wrapper_payload_type(
        repo_root=repo_root,
        return_type=extends_name,
        import_map=type_import_map,
        package_name=type_package_name,
        active_return_types=(*active_return_types, base_return_type),
    )


def _concrete_data_field_type(
    type_declaration: javalang.tree.ClassDeclaration,
) -> str | None:
    for field in type_declaration.fields:
        declarator_names = [declarator.name for declarator in field.declarators]
        if "data" not in declarator_names:
            continue
        java_type = _render_type(field.type)
        if java_type not in {"Object", "T"}:
            return java_type
    return None


def _base_java_type(java_type: str) -> str:
    base_type = java_type.split("<", 1)[0]
    if "." in base_type:
        return base_type.rsplit(".", 1)[-1]
    return base_type
