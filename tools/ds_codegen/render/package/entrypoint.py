from __future__ import annotations

import shutil
from collections import defaultdict
from typing import TYPE_CHECKING

from ds_codegen.ir import (
    ContractSnapshot,
    DtoSpec,
    EnumSpec,
    ModelSpec,
)
from ds_codegen.render.package.operations_renderer import (
    OperationRenderDeps as _OperationRenderDeps,
)
from ds_codegen.render.package.operations_renderer import (
    write_operations_modules as _write_operations_modules,
)
from ds_codegen.render.package.planner import (
    PackageRenderContext as _PackageRenderContext,
)
from ds_codegen.render.package.planner import (
    SpecializedModel as _SpecializedModel,
)
from ds_codegen.render.package.planner import (
    build_package_context as _build_package_context,
)
from ds_codegen.render.package.planner import (
    python_class_name as _python_class_name,
)
from ds_codegen.render.package.render_support import (
    display_doc_text as _display_doc_text,
)
from ds_codegen.render.package.render_support import (
    pydantic_field_name as _pydantic_field_name,
)
from ds_codegen.render.package.render_support import (
    render_docstring_lines as _render_docstring_lines,
)
from ds_codegen.render.package.render_support import (
    render_parameter_field_config as _render_parameter_field_config,
)
from ds_codegen.render.package.runtime_renderer import (
    write_base_operations_module as _write_base_operations_module,
)
from ds_codegen.render.package.runtime_renderer import (
    write_model_base_module as _write_model_base_module,
)
from ds_codegen.render.package.surface_renderer import (
    write_client_module as _write_client_module,
)
from ds_codegen.render.package.surface_renderer import (
    write_init_files as _write_init_files,
)
from ds_codegen.render.package.surface_renderer import (
    write_recursive_package_inits as _write_recursive_package_inits,
)
from ds_codegen.render.package.type_renderer import TypeRenderDeps as _TypeRenderDeps
from ds_codegen.render.package.type_renderer import (
    module_export_names as _module_export_names,
)
from ds_codegen.render.package.type_renderer import (
    render_type_module as _render_type_module,
)
from ds_codegen.render.package.type_support import (
    collect_annotation_import_targets as _collect_annotation_import_targets,
)
from ds_codegen.render.package.type_support import (
    field_annotation_type as _field_annotation_type,
)
from ds_codegen.render.package.type_support import (
    relative_import_statement as _relative_import_statement,
)
from ds_codegen.render.package.type_support import (
    render_annotation_type as _render_annotation_type,
)

if TYPE_CHECKING:
    from pathlib import Path
_REQUESTS_BASE_CLASS_NAME = "BaseRequestsClient"
_VERSION_ROOT_PREFIX = ("generated", "versions")
_RenderableSpec = EnumSpec | DtoSpec | ModelSpec
_ROOT_MODEL_MODULE_PARTS = ("_models",)
_BASE_CONTRACT_MODEL_NAME = "BaseContractModel"
_BASE_VIEW_MODEL_NAME = "BaseViewModel"
_BASE_ENTITY_MODEL_NAME = "BaseEntityModel"
_BASE_PARAMS_MODEL_NAME = "BaseParamsModel"


def _operation_render_deps() -> _OperationRenderDeps:
    return _OperationRenderDeps(
        requests_base_class_name=_REQUESTS_BASE_CLASS_NAME,
        base_params_model_name=_BASE_PARAMS_MODEL_NAME,
        collect_annotation_import_targets=_collect_annotation_import_targets,
        display_doc_text=_display_doc_text,
        field_annotation_type=_field_annotation_type,
        pydantic_field_name=_pydantic_field_name,
        python_class_name=_python_class_name,
        relative_import_statement=_relative_import_statement,
        render_annotation_type=_render_annotation_type,
        render_docstring_lines=_render_docstring_lines,
        render_parameter_field_config=_render_parameter_field_config,
    )


def _type_render_deps() -> _TypeRenderDeps:
    return _TypeRenderDeps(
        base_contract_model_name=_BASE_CONTRACT_MODEL_NAME,
        base_view_model_name=_BASE_VIEW_MODEL_NAME,
        base_entity_model_name=_BASE_ENTITY_MODEL_NAME,
        root_model_module_parts=_ROOT_MODEL_MODULE_PARTS,
    )


def write_generated_package(
    repo_root: Path,
    snapshot: ContractSnapshot,
    output_root: Path,
) -> None:
    version_package_parts = _version_package_parts(snapshot.ds_version)
    package_root = output_root.joinpath(*version_package_parts)
    if package_root.exists():
        shutil.rmtree(package_root)
    package_root.mkdir(parents=True, exist_ok=True)

    context = _build_package_context(repo_root, snapshot)
    _write_package_tree(package_root, version_package_parts, context)


def write_requests_package(
    repo_root: Path,
    snapshot: ContractSnapshot,
    output_root: Path,
) -> None:
    """Backward-compatible alias for generated package output."""
    write_generated_package(repo_root, snapshot, output_root)


def _write_package_tree(
    package_root: Path,
    version_package_parts: tuple[str, ...],
    context: _PackageRenderContext,
) -> None:
    modules: dict[tuple[str, ...], list[_RenderableSpec]] = defaultdict(list)
    seen_module_exports: set[tuple[tuple[str, ...], str]] = set()
    dto_specs_by_import_path = {
        dto_spec.import_path: dto_spec for dto_spec in context.snapshot.dtos
    }
    model_specs_by_import_path = {
        model_spec.import_path: model_spec for model_spec in context.snapshot.models
    }

    for enum_spec in context.snapshot.enums:
        assignment = context.assignments_by_import_path[enum_spec.import_path]
        module_key = (assignment.module_parts, assignment.class_name)
        if module_key in seen_module_exports:
            continue
        seen_module_exports.add(module_key)
        modules[assignment.module_parts].append(enum_spec)
    for dto_spec in context.snapshot.dtos:
        assignment = context.assignments_by_import_path[dto_spec.import_path]
        module_key = (assignment.module_parts, assignment.class_name)
        if module_key in seen_module_exports:
            continue
        seen_module_exports.add(module_key)
        modules[assignment.module_parts].append(dto_spec)
    for model_spec in context.snapshot.models:
        assignment = context.assignments_by_import_path[model_spec.import_path]
        module_key = (assignment.module_parts, assignment.class_name)
        if module_key in seen_module_exports:
            continue
        seen_module_exports.add(module_key)
        modules[assignment.module_parts].append(model_spec)

    specialized_by_module: dict[tuple[str, ...], list[_SpecializedModel]] = defaultdict(
        list
    )
    for specialized in context.specialized_by_java_type.values():
        specialized_by_module[specialized.module_parts].append(specialized)

    package_exports: dict[tuple[str, ...], dict[str, list[str]]] = defaultdict(dict)
    type_render_deps = _type_render_deps()
    _write_init_files(package_root)
    _write_model_base_module(
        package_root,
        base_contract_model_name=_BASE_CONTRACT_MODEL_NAME,
        base_view_model_name=_BASE_VIEW_MODEL_NAME,
        base_entity_model_name=_BASE_ENTITY_MODEL_NAME,
    )
    _write_base_operations_module(
        package_root,
        package_exports,
        requests_base_class_name=_REQUESTS_BASE_CLASS_NAME,
        base_params_model_name=_BASE_PARAMS_MODEL_NAME,
    )
    _write_operations_modules(
        package_root,
        context,
        package_exports,
        deps=_operation_render_deps(),
    )
    _write_client_module(package_root, version_package_parts, context)

    for module_parts, specs in sorted(modules.items()):
        specialized_models = specialized_by_module.get(module_parts, [])
        module_path = package_root.joinpath(*module_parts).with_suffix(".py")
        module_path.parent.mkdir(parents=True, exist_ok=True)
        module_path.write_text(
            _render_type_module(
                module_parts=module_parts,
                specs=specs,
                specialized_models=specialized_models,
                dto_specs_by_import_path=dto_specs_by_import_path,
                model_specs_by_import_path=model_specs_by_import_path,
                context=context,
                deps=type_render_deps,
            )
        )
        package_exports[module_parts[:-1]][module_parts[-1]] = _module_export_names(
            specs,
            specialized_models,
            context,
        )
    _write_recursive_package_inits(package_root, package_exports)


def _version_package_parts(ds_version: str) -> tuple[str, ...]:
    version_slug = f"ds_{ds_version.replace('.', '_')}"
    return (*_VERSION_ROOT_PREFIX, version_slug)
