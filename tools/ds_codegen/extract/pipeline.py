from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING

import javalang

from ds_codegen.extract import generated_view_support as _generated_view_support
from ds_codegen.extract import return_type_resolution as _return_type_resolution
from ds_codegen.extract import type_extraction as _type_extraction
from ds_codegen.extract.class_creator_inference import (
    ClassCreatorInferenceDeps as _ClassCreatorInferenceDeps,
)
from ds_codegen.extract.class_creator_inference import (
    find_constructor_declaration as _module_find_constructor_declaration,
)
from ds_codegen.extract.class_creator_inference import (
    infer_class_creator_return_type as _module_infer_class_creator_return_type,
)
from ds_codegen.extract.class_creator_inference import (
    infer_class_creator_structured_type as _module_infer_class_creator_structured_type,
)
from ds_codegen.extract.controller_contract import (
    ControllerExtractDeps as _ControllerExtractDeps,
)
from ds_codegen.extract.controller_contract import (
    deduplicate_operation_ids as _module_deduplicate_operation_ids,
)
from ds_codegen.extract.controller_contract import (
    extract_controller_contract as _module_extract_controller_contract,
)
from ds_codegen.extract.controller_contract import (
    iter_controller_paths as _module_iter_controller_paths,
)
from ds_codegen.extract.expression_inference import (
    ExpressionInferenceDeps as _ExpressionInferenceDeps,
)
from ds_codegen.extract.expression_inference import (
    infer_expression_data_type as _module_infer_expression_data_type,
)
from ds_codegen.extract.expression_inference import (
    infer_expression_return_type as _module_infer_expression_return_type,
)
from ds_codegen.extract.expression_inference import (
    infer_method_invocation_return_type as _module_infer_method_invocation_return_type,
)
from ds_codegen.extract.inference_support import (
    _collect_method_variable_initializers,
    _collect_method_variable_types,
    _extract_method_invocation_with_qualifier,
    _is_weak_inferred_type,
    _method_signature_key,
    _resolve_inferred_return_type,
)
from ds_codegen.extract.local_inference import (
    LocalInferenceDeps as _LocalInferenceDeps,
)
from ds_codegen.extract.local_inference import (
    infer_local_return_statement_payload_type as _module_infer_local_return_payload,
)
from ds_codegen.extract.local_inference import (
    infer_local_variable_payload_type as _module_infer_local_variable_payload,
)
from ds_codegen.extract.operation_inference import (
    OperationInferenceDeps as _OperationInferenceDeps,
)
from ds_codegen.extract.operation_inference import (
    infer_operation_return_type as _module_infer_operation_return_type,
)
from ds_codegen.extract.operation_inference import (
    infer_structured_return_statement_type as _module_infer_structured_return_type,
)
from ds_codegen.extract.return_type_resolution import (
    resolve_operation_response_projection as _module_resp_projection,
)
from ds_codegen.extract.return_type_resolution import (
    unwrap_result_like_type as _module_unwrap_result_like_type,
)
from ds_codegen.extract.service_inference import (
    ServiceInferenceDeps as _ServiceInferenceDeps,
)
from ds_codegen.extract.service_inference import (
    infer_same_class_method_payload_type as _module_infer_same_class_method_payload,
)
from ds_codegen.extract.service_inference import (
    infer_service_invocation_payload_type as _module_infer_service_invocation_payload,
)
from ds_codegen.extract.service_inference import (
    infer_service_method_payload_type as _module_infer_service_method_payload,
)
from ds_codegen.extract.service_inference import (
    is_data_list_expression as _module_is_data_list_expression,
)
from ds_codegen.extract.structure_inference import (
    StructureInferenceDeps as _StructureInferenceDeps,
)
from ds_codegen.extract.structure_inference import (
    infer_local_data_structure_type as _module_infer_local_data_structure_type,
)
from ds_codegen.extract.structure_inference import (
    infer_structured_expression_data_type as _module_infer_structured_expression_type,
)
from ds_codegen.extract.structure_inference import (
    is_collection_like_java_type as _module_is_collection_like_java_type,
)
from ds_codegen.extract.structure_inference import (
    normalized_collection_base_type as _module_normalized_collection_base_type,
)
from ds_codegen.extract.type_lookup import (
    _find_field_type_declaration,
    _find_field_type_declaration_in_hierarchy,
    _find_method_declaration,
    _java_generic_base_type,
    _java_generic_inner_types,
    _load_java_type_context,
    _render_reference_name,
    _render_type,
    _score_parameter_argument_match,
)
from ds_codegen.ir import (
    ContractSnapshot,
    DtoFieldSpec,
    DtoSpec,
    EnumSpec,
    ModelSpec,
    OperationSpec,
    ResponseProjection,
)
from ds_codegen.java_source import (
    load_type_declaration as _load_type_declaration,
)
from ds_codegen.java_source import (
    resolve_referenced_import_path as _resolve_referenced_import_path,
)
from ds_codegen.source import default_ds_source_root, read_ds_source_version

if TYPE_CHECKING:
    from pathlib import Path

DeclarationKind = _type_extraction.DeclarationKind

_GENERATED_VIEW_MODELS: dict[str, ModelSpec] = {}


def build_contract_snapshot(repo_root: Path) -> ContractSnapshot:
    _GENERATED_VIEW_MODELS.clear()
    operations: list[OperationSpec] = []
    enum_imports: set[str] = set()
    dto_imports: set[str] = set()
    model_imports: set[str] = set()
    parse_cache: dict[
        str,
        tuple[
            javalang.tree.CompilationUnit,
            javalang.tree.TypeDeclaration,
            dict[str, str],
            str | None,
        ]
        | None,
    ] = {}
    declaration_kind_cache: dict[str, DeclarationKind | None] = {}

    for controller_path in _iter_controller_paths(repo_root):
        (
            controller_operations,
            controller_enum_imports,
            controller_dto_imports,
            controller_model_imports,
        ) = _extract_controller_contract(repo_root, controller_path)
        operations.extend(controller_operations)
        enum_imports.update(controller_enum_imports)
        dto_imports.update(controller_dto_imports)
        model_imports.update(controller_model_imports)

    dto_specs, dto_model_imports, dto_enum_imports = _extract_dto_specs(
        repo_root,
        dto_imports,
        parse_cache,
        declaration_kind_cache,
    )
    model_imports.update(dto_model_imports)
    enum_imports.update(dto_enum_imports)
    model_specs, model_enum_imports, _ = _extract_model_specs(
        repo_root,
        model_imports,
        parse_cache,
        declaration_kind_cache,
    )
    enum_imports.update(model_enum_imports)
    enum_specs = _extract_enum_specs(repo_root, enum_imports, parse_cache)
    operations.sort(
        key=lambda item: (
            item.path,
            item.http_method,
            item.controller,
            item.method_name,
        )
    )
    operations = _deduplicate_operation_ids(operations)
    dto_specs.sort(key=lambda item: item.name)
    generated_view_models = list(_GENERATED_VIEW_MODELS.values())
    generated_view_models = _prune_unreferenced_generated_view_models(
        operations=operations,
        dto_specs=dto_specs,
        model_specs=model_specs,
        generated_view_models=generated_view_models,
    )
    additional_model_imports = _collect_generated_view_model_imports(
        repo_root,
        generated_view_models,
        {dto.name for dto in dto_specs}
        | {model.name for model in model_specs}
        | {enum_spec.name for enum_spec in enum_specs},
    )
    if additional_model_imports:
        additional_model_specs, additional_model_enum_imports, _ = _extract_model_specs(
            repo_root,
            additional_model_imports,
            parse_cache,
            declaration_kind_cache,
        )
        existing_model_names = {existing_model.name for existing_model in model_specs}
        model_specs.extend(
            model_spec
            for model_spec in additional_model_specs
            if model_spec.name not in existing_model_names
        )
        additional_enum_specs = _extract_enum_specs(
            repo_root,
            enum_imports | additional_model_enum_imports,
            parse_cache,
        )
        enum_specs = additional_enum_specs
    model_specs.extend(generated_view_models)
    model_specs.sort(key=lambda item: item.name)
    enum_specs.sort(key=lambda item: item.name)
    return ContractSnapshot(
        ds_version=read_ds_source_version(default_ds_source_root(repo_root)),
        operation_count=len(operations),
        enum_count=len(enum_specs),
        dto_count=len(dto_specs),
        model_count=len(model_specs),
        operations=operations,
        enums=enum_specs,
        dtos=dto_specs,
        models=model_specs,
    )


def _iter_controller_paths(repo_root: Path) -> list[Path]:
    return _module_iter_controller_paths(repo_root)


def _extract_controller_contract(
    repo_root: Path,
    controller_path: Path,
) -> tuple[list[OperationSpec], set[str], set[str], set[str]]:
    return _module_extract_controller_contract(
        repo_root=repo_root,
        controller_path=controller_path,
        deps=_controller_extract_deps(),
    )


def _deduplicate_operation_ids(
    operations: list[OperationSpec],
) -> list[OperationSpec]:
    return _module_deduplicate_operation_ids(operations)


def _prune_unreferenced_generated_view_models(
    *,
    operations: list[OperationSpec],
    dto_specs: list[DtoSpec],
    model_specs: list[ModelSpec],
    generated_view_models: list[ModelSpec],
) -> list[ModelSpec]:
    generated_view_names = {model.name for model in generated_view_models}
    generated_view_models_by_name = {
        model.name: model for model in generated_view_models
    }
    pending_names = list(
        _seed_generated_view_reference_names(
            operations=operations,
            dto_specs=dto_specs,
            model_specs=model_specs,
            generated_view_names=generated_view_names,
        )
    )
    reachable_names: set[str] = set()
    while pending_names:
        model_name = pending_names.pop()
        if model_name in reachable_names:
            continue
        model_spec = generated_view_models_by_name.get(model_name)
        if model_spec is None:
            continue
        reachable_names.add(model_name)
        pending_names.extend(
            _referenced_generated_view_names_from_model(
                model_spec,
                generated_view_names=generated_view_names,
            )
        )
    return [
        model_spec
        for model_spec in generated_view_models
        if model_spec.name in reachable_names
    ]


def _seed_generated_view_reference_names(
    *,
    operations: list[OperationSpec],
    dto_specs: list[DtoSpec],
    model_specs: list[ModelSpec],
    generated_view_names: set[str],
) -> set[str]:
    referenced_names: set[str] = set()
    for operation in operations:
        referenced_names.update(
            _referenced_generated_view_names_from_java_type(
                operation.logical_return_type,
                generated_view_names=generated_view_names,
            )
        )
    for dto_spec in dto_specs:
        referenced_names.update(
            _referenced_generated_view_names_from_fields(
                dto_spec.fields,
                generated_view_names=generated_view_names,
            )
        )
        if dto_spec.extends is not None:
            referenced_names.update(
                _referenced_generated_view_names_from_java_type(
                    dto_spec.extends,
                    generated_view_names=generated_view_names,
                )
            )
    for model_spec in model_specs:
        if model_spec.kind == "generated_view":
            continue
        referenced_names.update(
            _referenced_generated_view_names_from_fields(
                model_spec.fields,
                generated_view_names=generated_view_names,
            )
        )
        if model_spec.extends is not None:
            referenced_names.update(
                _referenced_generated_view_names_from_java_type(
                    model_spec.extends,
                    generated_view_names=generated_view_names,
                )
            )
    return referenced_names


def _referenced_generated_view_names_from_model(
    model_spec: ModelSpec,
    *,
    generated_view_names: set[str],
) -> set[str]:
    referenced_names = _referenced_generated_view_names_from_fields(
        model_spec.fields,
        generated_view_names=generated_view_names,
    )
    if model_spec.extends is not None:
        referenced_names.update(
            _referenced_generated_view_names_from_java_type(
                model_spec.extends,
                generated_view_names=generated_view_names,
            )
        )
    return referenced_names


def _referenced_generated_view_names_from_fields(
    fields: list[DtoFieldSpec],
    *,
    generated_view_names: set[str],
) -> set[str]:
    referenced_names: set[str] = set()
    for field in fields:
        referenced_names.update(
            _referenced_generated_view_names_from_java_type(
                field.java_type,
                generated_view_names=generated_view_names,
            )
        )
    return referenced_names


def _referenced_generated_view_names_from_java_type(
    java_type: str,
    *,
    generated_view_names: set[str],
) -> set[str]:
    return (
        _collect_type_reference_names_from_java_type(java_type) & generated_view_names
    )


def _infer_operation_return_type(
    *,
    repo_root: Path,
    controller_path: Path,
    method: javalang.tree.MethodDeclaration,
    controller_field_types: dict[str, str],
    import_map: dict[str, str],
    package_name: str | None,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    return _module_infer_operation_return_type(
        repo_root=repo_root,
        controller_path=controller_path,
        method=method,
        controller_field_types=controller_field_types,
        import_map=import_map,
        package_name=package_name,
        deps=_operation_inference_deps(),
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )


def _infer_structured_return_statement_type(
    *,
    repo_root: Path,
    controller_path: Path,
    method: javalang.tree.MethodDeclaration,
    return_statement: javalang.tree.ReturnStatement,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    return _module_infer_structured_return_type(
        repo_root=repo_root,
        controller_path=controller_path,
        method=method,
        return_statement=return_statement,
        controller_field_types=controller_field_types,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        import_map=import_map,
        package_name=package_name,
        deps=_operation_inference_deps(),
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )


def _operation_inference_deps() -> _OperationInferenceDeps:
    return _OperationInferenceDeps(
        infer_local_data_structure_type=_infer_local_data_structure_type,
        infer_local_variable_payload_type=_infer_local_variable_payload_type,
        infer_return_statement_type=_infer_return_statement_type,
        infer_local_return_statement_payload_type=(
            _infer_local_return_statement_payload_type
        ),
    )


def _expression_inference_deps() -> _ExpressionInferenceDeps:
    return _ExpressionInferenceDeps(
        infer_argument_types=_infer_argument_types,
        infer_service_invocation_payload_type=_infer_service_invocation_payload_type,
        infer_same_class_method_payload_type=_infer_same_class_method_payload_type,
        infer_success_like_invocation_payload_type=(
            _infer_success_like_invocation_payload_type
        ),
        infer_return_data_list_payload_type=_infer_return_data_list_payload_type,
        infer_class_creator_return_type=_infer_class_creator_return_type,
        infer_selector_chain_data_type=_infer_selector_chain_data_type,
        unwrap_result_like_type=_unwrap_result_like_type,
    )


def _local_inference_deps() -> _LocalInferenceDeps:
    return _LocalInferenceDeps(
        infer_expression_return_type=_infer_expression_return_type,
        infer_expression_data_type=_infer_expression_data_type,
        infer_local_data_structure_type=_infer_local_data_structure_type,
        collect_method_variable_types=_collect_method_variable_types,
        collect_method_variable_initializers=_collect_method_variable_initializers,
        infer_argument_types=_infer_argument_types,
        find_method_declaration=_find_method_declaration,
        method_signature_key=_method_signature_key,
        resolve_inferred_return_type=_resolve_inferred_return_type,
        is_weak_inferred_type=_is_weak_inferred_type,
        is_collection_like_java_type=_is_collection_like_java_type,
        is_data_list_expression=_is_data_list_expression,
        unwrap_generated_view_data_list_type=_unwrap_generated_view_data_list_type,
        render_reference_name=_render_reference_name,
        resolve_referenced_import_path=_resolve_referenced_import_path,
        type_extends_result=_type_extends_result,
    )


def _structure_inference_deps() -> _StructureInferenceDeps:
    return _StructureInferenceDeps(
        infer_local_variable_payload_type=_infer_local_variable_payload_type,
        infer_expression_data_type=_infer_expression_data_type,
        resolve_string_constant_value=_resolve_string_constant_value,
        register_generated_view_model=_register_generated_view_model,
        get_generated_view_model_fields=_generated_view_model_fields,
        is_weak_inferred_type=_is_weak_inferred_type,
    )


def _controller_extract_deps() -> _ControllerExtractDeps:
    return _ControllerExtractDeps(
        infer_operation_return_type=_infer_operation_return_type,
        resolve_operation_logical_return_type=_resolve_operation_logical_return_type,
        resolve_operation_response_projection=_resolve_operation_response_projection,
        resolve_referenced_import_path=_resolve_referenced_import_path,
        looks_like_request_dto_import=_looks_like_request_dto_import,
        collect_type_reference_names=_collect_type_reference_names,
        collect_type_reference_names_from_java_type=(
            _collect_type_reference_names_from_java_type
        ),
    )


def _class_creator_inference_deps() -> _ClassCreatorInferenceDeps:
    return _ClassCreatorInferenceDeps(
        render_reference_name=_render_reference_name,
        infer_expression_data_type=_infer_expression_data_type,
        infer_expression_return_type=_infer_expression_return_type,
        infer_argument_types=_infer_argument_types,
        resolve_referenced_import_path=_resolve_referenced_import_path,
        type_extends_result=_type_extends_result,
        load_java_type_context=_load_java_type_context,
        find_constructor_declaration=_find_constructor_declaration,
        find_field_type_declaration_in_hierarchy=(
            _find_field_type_declaration_in_hierarchy
        ),
        register_generated_view_model=_register_generated_view_model,
    )


def _infer_return_statement_type(
    *,
    repo_root: Path,
    controller_path: Path,
    return_statement: javalang.tree.ReturnStatement,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    return _infer_expression_return_type(
        repo_root=repo_root,
        controller_path=controller_path,
        expression=return_statement.expression,
        controller_field_types=controller_field_types,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        import_map=import_map,
        package_name=package_name,
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )


def _infer_expression_return_type(
    *,
    repo_root: Path,
    controller_path: Path,
    expression: object | None,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    return _module_infer_expression_return_type(
        repo_root=repo_root,
        controller_path=controller_path,
        expression=expression,
        controller_field_types=controller_field_types,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        import_map=import_map,
        package_name=package_name,
        deps=_expression_inference_deps(),
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )


def _infer_method_invocation_return_type(
    *,
    repo_root: Path,
    controller_path: Path,
    invocation: javalang.tree.MethodInvocation,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    qualifier_override: str | None = None,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    return _module_infer_method_invocation_return_type(
        repo_root=repo_root,
        controller_path=controller_path,
        invocation=invocation,
        controller_field_types=controller_field_types,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        import_map=import_map,
        package_name=package_name,
        deps=_expression_inference_deps(),
        qualifier_override=qualifier_override,
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )


def _infer_success_like_invocation_payload_type(
    *,
    repo_root: Path,
    controller_path: Path,
    invocation: javalang.tree.MethodInvocation,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    if not invocation.arguments:
        return "Void"
    if invocation.member == "getResult":
        if len(invocation.arguments) < 2:
            return None
        return _infer_expression_data_type(
            repo_root=repo_root,
            controller_path=controller_path,
            expression=invocation.arguments[1],
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
    if len(invocation.arguments) == 1:
        return _infer_expression_data_type(
            repo_root=repo_root,
            controller_path=controller_path,
            expression=invocation.arguments[0],
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
    if len(invocation.arguments) == 2:
        return _infer_expression_data_type(
            repo_root=repo_root,
            controller_path=controller_path,
            expression=invocation.arguments[1],
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
    return None


def _infer_class_creator_return_type(
    *,
    repo_root: Path,
    controller_path: Path,
    class_creator: javalang.tree.ClassCreator,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    return _module_infer_class_creator_return_type(
        repo_root=repo_root,
        controller_path=controller_path,
        class_creator=class_creator,
        controller_field_types=controller_field_types,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        import_map=import_map,
        package_name=package_name,
        deps=_class_creator_inference_deps(),
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )


def _infer_class_creator_structured_type(
    *,
    repo_root: Path,
    controller_path: Path,
    class_creator: javalang.tree.ClassCreator,
    view_name_hint: str,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    return _module_infer_class_creator_structured_type(
        repo_root=repo_root,
        controller_path=controller_path,
        class_creator=class_creator,
        view_name_hint=view_name_hint,
        controller_field_types=controller_field_types,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        import_map=import_map,
        package_name=package_name,
        deps=_class_creator_inference_deps(),
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )


def _find_constructor_declaration(
    constructors: list[javalang.tree.ConstructorDeclaration],
    constructor_arity: int,
    argument_types: list[str | None] | None = None,
) -> javalang.tree.ConstructorDeclaration | None:
    return _module_find_constructor_declaration(
        constructors,
        constructor_arity,
        argument_types,
        _score_parameter_argument_match,
    )


@cache
def _type_extends_result(repo_root: Path, import_path: str) -> bool:
    parse_cache: dict[
        str,
        tuple[
            javalang.tree.CompilationUnit,
            javalang.tree.TypeDeclaration,
            dict[str, str],
            str | None,
        ]
        | None,
    ] = {}
    loaded_declaration = _load_type_declaration(repo_root, import_path, parse_cache)
    if loaded_declaration is None:
        return False
    _, type_declaration, _, _ = loaded_declaration
    if not isinstance(type_declaration, javalang.tree.ClassDeclaration):
        return False
    return (
        type_declaration.extends is not None
        and _render_reference_name(type_declaration.extends) == "Result"
    )


def _infer_expression_data_type(
    *,
    repo_root: Path,
    controller_path: Path,
    expression: object,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    return _module_infer_expression_data_type(
        repo_root=repo_root,
        controller_path=controller_path,
        expression=expression,
        controller_field_types=controller_field_types,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        import_map=import_map,
        package_name=package_name,
        deps=_expression_inference_deps(),
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )


def _infer_selector_chain_data_type(
    *,
    repo_root: Path,
    controller_path: Path,
    base_java_type: str,
    selectors: list[object],
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    if not selectors:
        return None
    current_java_type = base_java_type
    current_import_map = import_map
    current_package_name = package_name
    for selector in selectors:
        if isinstance(selector, javalang.tree.MethodInvocation):
            stream_selector_type = _infer_stream_selector_return_type(
                current_java_type=current_java_type,
                selector=selector,
            )
            if stream_selector_type is not None:
                current_java_type = stream_selector_type
                continue
            loaded_type = _load_java_type_context(
                repo_root=repo_root,
                java_type=current_java_type,
                import_map=current_import_map,
                package_name=current_package_name,
            )
            if loaded_type is None:
                return None
            _, type_declaration, type_import_map, type_package_name = loaded_type
            method_declaration = _find_method_declaration(
                type_declaration.methods,
                selector.member,
                len(selector.arguments),
                argument_types=_infer_argument_types(
                    repo_root=repo_root,
                    controller_path=controller_path,
                    arguments=selector.arguments,
                    controller_field_types=controller_field_types,
                    variable_types=variable_types,
                    variable_initializers=variable_initializers,
                    import_map=import_map,
                    package_name=package_name,
                    owner_methods=owner_methods,
                    active_same_class_methods=active_same_class_methods,
                ),
            )
            if method_declaration is None:
                return None
            current_java_type = _render_type(method_declaration.return_type)
            current_import_map = type_import_map
            current_package_name = type_package_name
            continue
        if isinstance(selector, javalang.tree.MemberReference):
            loaded_type = _load_java_type_context(
                repo_root=repo_root,
                java_type=current_java_type,
                import_map=current_import_map,
                package_name=current_package_name,
            )
            if loaded_type is None:
                return None
            _, type_declaration, type_import_map, type_package_name = loaded_type
            field_type = _find_field_type_declaration(
                type_declaration,
                selector.member,
            )
            if field_type is None:
                return None
            current_java_type = field_type
            current_import_map = type_import_map
            current_package_name = type_package_name
            continue
        return None
    return current_java_type


def _infer_stream_selector_return_type(
    *,
    current_java_type: str,
    selector: javalang.tree.MethodInvocation,
) -> str | None:
    if _java_generic_base_type(current_java_type) != "Stream":
        return None
    if (
        selector.member
        in {
            "distinct",
            "parallel",
            "sequential",
            "sorted",
            "unordered",
        }
        and not selector.arguments
    ):
        return current_java_type
    if selector.member != "collect" or len(selector.arguments) != 1:
        return None
    stream_inner_types = _java_generic_inner_types(current_java_type)
    stream_item_type = stream_inner_types[0] if stream_inner_types else "Object"
    collector_expression = selector.arguments[0]
    if not isinstance(collector_expression, javalang.tree.MethodInvocation):
        return None
    if collector_expression.qualifier != "Collectors":
        return None
    if collector_expression.member == "toList":
        return f"List<{stream_item_type}>"
    if collector_expression.member == "toSet":
        return f"Set<{stream_item_type}>"
    return None


def _infer_local_return_statement_payload_type(
    *,
    repo_root: Path,
    controller_path: Path,
    method: javalang.tree.MethodDeclaration,
    return_statement: javalang.tree.ReturnStatement,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    return _module_infer_local_return_payload(
        repo_root=repo_root,
        controller_path=controller_path,
        method=method,
        return_statement=return_statement,
        controller_field_types=controller_field_types,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        import_map=import_map,
        package_name=package_name,
        deps=_local_inference_deps(),
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )


def _infer_local_variable_payload_type(
    *,
    repo_root: Path,
    controller_path: Path,
    method: javalang.tree.MethodDeclaration,
    variable_name: str,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    return _module_infer_local_variable_payload(
        repo_root=repo_root,
        controller_path=controller_path,
        method=method,
        variable_name=variable_name,
        controller_field_types=controller_field_types,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        import_map=import_map,
        package_name=package_name,
        deps=_local_inference_deps(),
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )


def _unwrap_result_like_type(java_type: str) -> str | None:
    return _module_unwrap_result_like_type(java_type)


def _resolve_operation_logical_return_type(
    *,
    repo_root: Path,
    raw_return_type: str,
    inferred_return_type: str | None,
    import_map: dict[str, str],
    package_name: str | None,
) -> str:
    return _return_type_resolution.resolve_operation_logical_return_type(
        repo_root=repo_root,
        raw_return_type=raw_return_type,
        inferred_return_type=inferred_return_type,
        import_map=import_map,
        package_name=package_name,
    )


def _resolve_operation_response_projection(
    *,
    raw_return_type: str,
    logical_return_type: str,
) -> ResponseProjection:
    projection = _module_resp_projection(
        raw_return_type=raw_return_type,
        logical_return_type=logical_return_type,
    )
    if projection != "direct":
        return projection
    fields = _generated_view_model_fields(logical_return_type)
    if fields is None or len(fields) != 1:
        return "direct"
    field_name, _field_type = fields[0]
    if field_name == "data":
        return "single_data"
    if field_name == "dataList":
        return "single_data_list"
    return "direct"


def _infer_return_data_list_payload_type(
    *,
    repo_root: Path,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    invocation: javalang.tree.MethodInvocation,
    controller_path: Path,
    import_map: dict[str, str],
    package_name: str | None,
) -> str | None:
    if not invocation.arguments:
        return None
    argument = invocation.arguments[0]
    direct_service_invocation = _extract_method_invocation_with_qualifier(argument)
    if direct_service_invocation is not None:
        direct_invocation, qualifier = direct_service_invocation
        if qualifier in controller_field_types:
            return _infer_service_invocation_payload_type(
                repo_root=repo_root,
                service_field_type=controller_field_types[qualifier],
                service_method_name=direct_invocation.member,
                service_method_arity=len(direct_invocation.arguments),
                argument_types=_infer_argument_types(
                    repo_root=repo_root,
                    controller_path=controller_path,
                    arguments=direct_invocation.arguments,
                    controller_field_types=controller_field_types,
                    variable_types=variable_types,
                    variable_initializers=variable_initializers,
                    import_map=import_map,
                    package_name=package_name,
                ),
                import_map=import_map,
                package_name=package_name,
            )
    if not isinstance(argument, javalang.tree.MemberReference):
        return None
    initializer = variable_initializers.get(argument.member)
    extracted_invocation = _extract_method_invocation_with_qualifier(initializer)
    if extracted_invocation is None:
        return None
    initializer_invocation, qualifier = extracted_invocation
    if qualifier not in controller_field_types:
        return None
    return _infer_service_invocation_payload_type(
        repo_root=repo_root,
        service_field_type=controller_field_types[qualifier],
        service_method_name=initializer_invocation.member,
        service_method_arity=len(initializer_invocation.arguments),
        argument_types=_infer_argument_types(
            repo_root=repo_root,
            controller_path=controller_path,
            arguments=initializer_invocation.arguments,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
        ),
        import_map=import_map,
        package_name=package_name,
    )


def _infer_service_invocation_payload_type(
    *,
    repo_root: Path,
    service_field_type: str,
    service_method_name: str,
    service_method_arity: int,
    argument_types: list[str | None] | None,
    import_map: dict[str, str],
    package_name: str | None,
) -> str | None:
    return _module_infer_service_invocation_payload(
        repo_root=repo_root,
        service_field_type=service_field_type,
        service_method_name=service_method_name,
        service_method_arity=service_method_arity,
        argument_types=argument_types,
        import_map=import_map,
        package_name=package_name,
        deps=_service_inference_deps(),
    )


def _infer_same_class_method_payload_type(
    *,
    repo_root: Path,
    controller_path: Path,
    owner_methods: list[javalang.tree.MethodDeclaration],
    method_name: str,
    method_arity: int,
    argument_types: list[str | None] | None,
    controller_field_types: dict[str, str],
    import_map: dict[str, str],
    package_name: str | None,
    active_same_class_methods: tuple[tuple[str, int], ...],
) -> str | None:
    return _module_infer_same_class_method_payload(
        repo_root=repo_root,
        controller_path=controller_path,
        owner_methods=owner_methods,
        method_name=method_name,
        method_arity=method_arity,
        argument_types=argument_types,
        controller_field_types=controller_field_types,
        import_map=import_map,
        package_name=package_name,
        deps=_service_inference_deps(),
        active_same_class_methods=active_same_class_methods,
    )


def _infer_service_method_payload_type(
    *,
    repo_root: Path,
    controller_path: Path,
    service_method: javalang.tree.MethodDeclaration,
    service_owner_methods: list[javalang.tree.MethodDeclaration],
    service_field_types: dict[str, str],
    service_import_map: dict[str, str],
    service_package_name: str | None,
    active_service_methods: tuple[tuple[str, int], ...],
) -> str | None:
    return _module_infer_service_method_payload(
        repo_root=repo_root,
        controller_path=controller_path,
        service_method=service_method,
        service_owner_methods=service_owner_methods,
        service_field_types=service_field_types,
        service_import_map=service_import_map,
        service_package_name=service_package_name,
        deps=_service_inference_deps(),
        active_service_methods=active_service_methods,
    )


def _is_data_list_expression(expression: object) -> bool:
    return _module_is_data_list_expression(expression)


def _service_inference_deps() -> _ServiceInferenceDeps:
    return _ServiceInferenceDeps(
        infer_argument_types=_infer_argument_types,
        infer_expression_data_type=_infer_expression_data_type,
        infer_local_data_structure_type=_infer_local_data_structure_type,
        infer_operation_return_type=_infer_operation_return_type,
        unwrap_generated_view_data_list_type=_unwrap_generated_view_data_list_type,
        is_data_list_expression=_module_is_data_list_expression,
    )


def _find_field_declaration(
    type_declaration: javalang.tree.TypeDeclaration,
    field_name: str,
) -> javalang.tree.FieldDeclaration | None:
    return _generated_view_support.find_field_declaration(type_declaration, field_name)


def _find_field_declaration_in_hierarchy(
    *,
    repo_root: Path,
    type_declaration: javalang.tree.TypeDeclaration,
    import_map: dict[str, str],
    package_name: str | None,
    field_name: str,
    active_import_paths: tuple[str, ...] = (),
) -> javalang.tree.FieldDeclaration | None:
    return _generated_view_support.find_field_declaration_in_hierarchy(
        repo_root=repo_root,
        type_declaration=type_declaration,
        import_map=import_map,
        package_name=package_name,
        field_name=field_name,
        active_import_paths=active_import_paths,
    )


def _resolve_string_constant_value(
    *,
    repo_root: Path,
    expression: object,
    import_map: dict[str, str],
    package_name: str | None,
    controller_path: Path | None = None,
    owner_type_declaration: javalang.tree.TypeDeclaration | None = None,
) -> str | None:
    return _generated_view_support.resolve_string_constant_value(
        repo_root=repo_root,
        expression=expression,
        import_map=import_map,
        package_name=package_name,
        controller_path=controller_path,
        owner_type_declaration=owner_type_declaration,
    )


def _load_static_string_constant_value(
    *,
    repo_root: Path,
    constant_import_path: str,
    constant_name: str,
) -> str | None:
    return _generated_view_support.load_static_string_constant_value(
        repo_root=repo_root,
        constant_import_path=constant_import_path,
        constant_name=constant_name,
    )


def _decode_string_field_initializer(
    field: javalang.tree.FieldDeclaration,
) -> str | None:
    return _generated_view_support.decode_string_field_initializer(field)


def _register_generated_view_model(
    *,
    base_name: str,
    fields: list[tuple[str, str]],
) -> str:
    return _generated_view_support.register_generated_view_model(
        generated_view_models=_GENERATED_VIEW_MODELS,
        base_name=base_name,
        fields=fields,
    )


def _unwrap_generated_view_data_list_type(java_type: str | None) -> str | None:
    return _generated_view_support.unwrap_generated_view_data_list_type(
        _GENERATED_VIEW_MODELS,
        java_type,
    )


def _generated_view_model_fields(model_name: str) -> list[tuple[str, str]] | None:
    return _generated_view_support.generated_view_model_fields(
        _GENERATED_VIEW_MODELS,
        model_name,
    )


def _infer_argument_types(
    *,
    repo_root: Path,
    controller_path: Path,
    arguments: list[object],
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> list[str | None]:
    return [
        _infer_expression_data_type(
            repo_root=repo_root,
            controller_path=controller_path,
            expression=argument,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
        for argument in arguments
    ]


def _infer_local_data_structure_type(
    *,
    repo_root: Path,
    controller_path: Path,
    method: javalang.tree.MethodDeclaration,
    variable_name: str,
    view_name_hint: str,
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    controller_field_types: dict[str, str],
    import_map: dict[str, str],
    package_name: str | None,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
    active_variables: tuple[str, ...] = (),
) -> str | None:
    return _module_infer_local_data_structure_type(
        repo_root=repo_root,
        controller_path=controller_path,
        method=method,
        variable_name=variable_name,
        view_name_hint=view_name_hint,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        controller_field_types=controller_field_types,
        import_map=import_map,
        package_name=package_name,
        deps=_structure_inference_deps(),
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
        active_variables=active_variables,
    )


def _is_collection_like_java_type(java_type: str) -> bool:
    return _module_is_collection_like_java_type(java_type)


def _normalized_collection_base_type(java_type: str) -> str | None:
    return _module_normalized_collection_base_type(java_type)


def _infer_structured_expression_data_type(
    *,
    repo_root: Path,
    controller_path: Path,
    method: javalang.tree.MethodDeclaration,
    expression: object,
    view_name_hint: str,
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    controller_field_types: dict[str, str],
    import_map: dict[str, str],
    package_name: str | None,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
    active_variables: tuple[str, ...] = (),
) -> str | None:
    return _module_infer_structured_expression_type(
        repo_root=repo_root,
        controller_path=controller_path,
        method=method,
        expression=expression,
        view_name_hint=view_name_hint,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        controller_field_types=controller_field_types,
        import_map=import_map,
        package_name=package_name,
        deps=_structure_inference_deps(),
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
        active_variables=active_variables,
    )


def _extract_dto_specs(
    repo_root: Path,
    import_paths: set[str],
    parse_cache: dict[
        str,
        tuple[
            javalang.tree.CompilationUnit,
            javalang.tree.TypeDeclaration,
            dict[str, str],
            str | None,
        ]
        | None,
    ],
    declaration_kind_cache: dict[str, DeclarationKind | None],
) -> tuple[list[DtoSpec], set[str], set[str]]:
    return _type_extraction.extract_dto_specs(
        repo_root,
        import_paths,
        parse_cache,
        declaration_kind_cache,
    )


def _extract_model_specs(
    repo_root: Path,
    import_paths: set[str],
    parse_cache: dict[
        str,
        tuple[
            javalang.tree.CompilationUnit,
            javalang.tree.TypeDeclaration,
            dict[str, str],
            str | None,
        ]
        | None,
    ],
    declaration_kind_cache: dict[str, DeclarationKind | None],
) -> tuple[list[ModelSpec], set[str], set[str]]:
    return _type_extraction.extract_model_specs(
        repo_root,
        import_paths,
        parse_cache,
        declaration_kind_cache,
    )


def _extract_enum_specs(
    repo_root: Path,
    import_paths: set[str],
    parse_cache: dict[
        str,
        tuple[
            javalang.tree.CompilationUnit,
            javalang.tree.TypeDeclaration,
            dict[str, str],
            str | None,
        ]
        | None,
    ],
) -> list[EnumSpec]:
    return _type_extraction.extract_enum_specs(repo_root, import_paths, parse_cache)


def _collect_generated_view_model_imports(
    repo_root: Path,
    generated_view_models: list[ModelSpec],
    known_type_names: set[str],
) -> set[str]:
    return _type_extraction.collect_generated_view_model_imports(
        repo_root,
        generated_view_models,
        known_type_names,
    )


def _collect_type_reference_names(type_node: object | None) -> set[str]:
    return _type_extraction.collect_type_reference_names(type_node)


def _collect_type_reference_names_from_java_type(java_type: str) -> set[str]:
    return _type_extraction.collect_type_reference_names_from_java_type(java_type)


def _looks_like_request_dto_import(import_path: str) -> bool:
    return _type_extraction.looks_like_request_dto_import(import_path)
