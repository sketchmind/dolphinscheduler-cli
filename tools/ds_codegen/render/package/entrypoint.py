from __future__ import annotations

import shutil
from collections import defaultdict
from typing import TYPE_CHECKING

from ds_codegen.ir import ContractSnapshot, DtoSpec, EnumSpec, ModelSpec
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
from ds_codegen.render.package.renderer_deps import (
    BASE_CONTRACT_MODEL_NAME as _BASE_CONTRACT_MODEL_NAME,
)
from ds_codegen.render.package.renderer_deps import (
    BASE_ENTITY_MODEL_NAME as _BASE_ENTITY_MODEL_NAME,
)
from ds_codegen.render.package.renderer_deps import (
    BASE_PARAMS_MODEL_NAME as _BASE_PARAMS_MODEL_NAME,
)
from ds_codegen.render.package.renderer_deps import (
    BASE_VIEW_MODEL_NAME as _BASE_VIEW_MODEL_NAME,
)
from ds_codegen.render.package.renderer_deps import (
    REQUESTS_BASE_CLASS_NAME as _REQUESTS_BASE_CLASS_NAME,
)
from ds_codegen.render.package.renderer_deps import (
    operation_render_deps as _operation_render_deps,
)
from ds_codegen.render.package.renderer_deps import (
    type_render_deps as _type_render_deps,
)
from ds_codegen.render.package.runtime_renderer import (
    write_base_operations_facade_module as _write_base_operations_facade_module,
)
from ds_codegen.render.package.runtime_renderer import (
    write_base_operations_module as _write_base_operations_module,
)
from ds_codegen.render.package.runtime_renderer import (
    write_model_base_facade_module as _write_model_base_facade_module,
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
from ds_codegen.render.package.type_renderer import (
    module_export_names as _module_export_names,
)
from ds_codegen.render.package.type_renderer import (
    render_type_module as _render_type_module,
)

if TYPE_CHECKING:
    from pathlib import Path

_VERSION_ROOT_PREFIX = ("generated", "versions")
_RenderableSpec = EnumSpec | DtoSpec | ModelSpec


def write_generated_package(
    snapshot: ContractSnapshot,
    output_root: Path,
    *,
    shared_runtime: bool = False,
) -> None:
    version_package_parts = _version_package_parts(snapshot.ds_version)
    package_root = output_root.joinpath(*version_package_parts)
    if package_root.exists():
        shutil.rmtree(package_root)
    package_root.mkdir(parents=True, exist_ok=True)

    context = _build_package_context(snapshot)
    _write_package_tree(
        package_root,
        version_package_parts,
        context,
        shared_runtime=shared_runtime,
    )


def _write_package_tree(
    package_root: Path,
    version_package_parts: tuple[str, ...],
    context: _PackageRenderContext,
    *,
    shared_runtime: bool = False,
) -> None:
    declaration_only_runtime = shared_runtime and not context.snapshot.operations
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
    if not declaration_only_runtime:
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
    if not declaration_only_runtime:
        for specialized in context.specialized_by_java_type.values():
            specialized_by_module[specialized.module_parts].append(specialized)

    package_exports: dict[tuple[str, ...], dict[str, list[str]]] = defaultdict(dict)
    type_render_deps = _type_render_deps()
    _write_init_files(package_root, export_client=not declaration_only_runtime)
    if not declaration_only_runtime:
        model_base_writer = (
            _write_model_base_module
            if not shared_runtime
            else _write_model_base_facade_module
        )
        model_base_writer(
            package_root,
            base_contract_model_name=_BASE_CONTRACT_MODEL_NAME,
            base_view_model_name=_BASE_VIEW_MODEL_NAME,
            base_entity_model_name=_BASE_ENTITY_MODEL_NAME,
        )
        base_writer = (
            _write_base_operations_module
            if not shared_runtime
            else _write_base_operations_facade_module
        )
        base_writer(
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
