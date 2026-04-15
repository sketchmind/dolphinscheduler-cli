"""Service-focused return inference extracted from the main extractor."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import javalang

from ds_codegen.extract.inference_support import (
    _collect_class_field_types,
    _collect_method_variable_initializers,
    _collect_method_variable_types,
    _extract_method_invocation_with_qualifier,
    _method_signature_key,
    _resolve_inferred_return_type,
)
from ds_codegen.extract.return_type_resolution import unwrap_result_like_type
from ds_codegen.extract.type_lookup import (
    _find_method_declaration,
    _load_cached_type_declaration,
    _render_type,
    _resolve_service_impl_import_path,
)
from ds_codegen.java_source import (
    resolve_import_path,
    resolve_referenced_import_path,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


@dataclass(frozen=True)
class ServiceInferenceDeps:
    infer_argument_types: Callable[..., list[str | None]]
    infer_expression_data_type: Callable[..., str | None]
    infer_local_data_structure_type: Callable[..., str | None]
    infer_operation_return_type: Callable[..., str | None]
    unwrap_generated_view_data_list_type: Callable[[str | None], str | None]
    is_data_list_expression: Callable[[object], bool]


def infer_service_invocation_payload_type(
    *,
    repo_root: Path,
    service_field_type: str,
    service_method_name: str,
    service_method_arity: int,
    argument_types: list[str | None] | None,
    import_map: dict[str, str],
    package_name: str | None,
    deps: ServiceInferenceDeps,
) -> str | None:
    service_import_path = resolve_referenced_import_path(
        repo_root,
        service_field_type,
        import_map,
        package_name,
    )
    if service_import_path is None:
        return None
    service_impl_import_path = _resolve_service_impl_import_path(
        repo_root,
        service_import_path,
    )
    if service_impl_import_path is None:
        return None
    loaded_declaration = _load_cached_type_declaration(
        repo_root,
        service_impl_import_path,
    )
    if loaded_declaration is None:
        return None
    _, type_declaration, service_import_map, service_package_name = loaded_declaration
    if not isinstance(type_declaration, javalang.tree.ClassDeclaration):
        return None
    service_method = _find_method_declaration(
        type_declaration.methods,
        service_method_name,
        service_method_arity,
        argument_types=argument_types,
    )
    if service_method is None:
        return None
    service_field_types = _collect_class_field_types(type_declaration)
    return infer_service_method_payload_type(
        repo_root=repo_root,
        controller_path=resolve_import_path(repo_root, service_impl_import_path)
        or repo_root,
        service_method=service_method,
        service_owner_methods=type_declaration.methods,
        service_field_types=service_field_types,
        service_import_map=service_import_map,
        service_package_name=service_package_name,
        deps=deps,
        active_service_methods=(),
    )


def infer_same_class_method_payload_type(
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
    deps: ServiceInferenceDeps,
    active_same_class_methods: tuple[tuple[str, int], ...],
) -> str | None:
    delegated_method = _find_method_declaration(
        owner_methods,
        method_name,
        method_arity,
        argument_types=argument_types,
    )
    if delegated_method is None:
        return None
    method_key = _method_signature_key(delegated_method)
    if method_key in active_same_class_methods:
        return None
    return deps.infer_operation_return_type(
        repo_root=repo_root,
        controller_path=controller_path,
        method=delegated_method,
        controller_field_types=controller_field_types,
        import_map=import_map,
        package_name=package_name,
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )


def infer_service_method_payload_type(
    *,
    repo_root: Path,
    controller_path: Path,
    service_method: javalang.tree.MethodDeclaration,
    service_owner_methods: list[javalang.tree.MethodDeclaration],
    service_field_types: dict[str, str],
    service_import_map: dict[str, str],
    service_package_name: str | None,
    deps: ServiceInferenceDeps,
    active_service_methods: tuple[tuple[str, int], ...],
) -> str | None:
    service_method_key = _method_signature_key(service_method)
    if service_method_key in active_service_methods:
        return None
    nested_active_service_methods = (*active_service_methods, service_method_key)
    service_return_type = _render_type(service_method.return_type)
    unwrapped_return_type = unwrap_result_like_type(service_return_type)
    if unwrapped_return_type not in {None, "Object"}:
        return unwrapped_return_type

    variable_types = _collect_method_variable_types(service_method)
    variable_initializers = _collect_method_variable_initializers(service_method)

    if service_return_type.startswith("Map<"):
        data_assignment_types = collect_service_data_assignment_types(
            repo_root=repo_root,
            controller_path=controller_path,
            service_method=service_method,
            service_field_types=service_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=service_import_map,
            package_name=service_package_name,
            assignment_kind="data_list",
            deps=deps,
            owner_methods=service_owner_methods,
            active_same_class_methods=nested_active_service_methods,
        )
        inferred_return_type = _resolve_inferred_return_type(
            data_assignment_types,
            saw_success_like_void=False,
        )
        if inferred_return_type is not None:
            return inferred_return_type

    if service_return_type in {"Result", "Result<Object>"}:
        data_assignment_types = collect_service_data_assignment_types(
            repo_root=repo_root,
            controller_path=controller_path,
            service_method=service_method,
            service_field_types=service_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=service_import_map,
            package_name=service_package_name,
            assignment_kind="set_data",
            deps=deps,
            owner_methods=service_owner_methods,
            active_same_class_methods=nested_active_service_methods,
        )
        inferred_return_type = _resolve_inferred_return_type(
            data_assignment_types,
            saw_success_like_void=False,
        )
        if inferred_return_type is not None:
            return inferred_return_type

    delegated_return_type = infer_same_class_delegated_return_type(
        repo_root=repo_root,
        controller_path=controller_path,
        service_method=service_method,
        service_owner_methods=service_owner_methods,
        service_field_types=service_field_types,
        service_import_map=service_import_map,
        service_package_name=service_package_name,
        deps=deps,
        active_service_methods=nested_active_service_methods,
    )
    if delegated_return_type is not None:
        return delegated_return_type

    return deps.infer_operation_return_type(
        repo_root=repo_root,
        controller_path=controller_path,
        method=service_method,
        controller_field_types=service_field_types,
        import_map=service_import_map,
        package_name=service_package_name,
        owner_methods=service_owner_methods,
        active_same_class_methods=active_service_methods,
    )


def infer_same_class_delegated_return_type(
    *,
    repo_root: Path,
    controller_path: Path,
    service_method: javalang.tree.MethodDeclaration,
    service_owner_methods: list[javalang.tree.MethodDeclaration],
    service_field_types: dict[str, str],
    service_import_map: dict[str, str],
    service_package_name: str | None,
    deps: ServiceInferenceDeps,
    active_service_methods: tuple[tuple[str, int], ...],
) -> str | None:
    for _, return_statement in service_method.filter(javalang.tree.ReturnStatement):
        expression = return_statement.expression
        extracted_invocation = _extract_method_invocation_with_qualifier(expression)
        if extracted_invocation is None:
            continue
        invocation, qualifier = extracted_invocation
        if qualifier not in {"", "this"}:
            continue
        if invocation.member in {
            "error",
            "errorWithArgs",
            "getResult",
            "returnDataList",
            "success",
        }:
            continue
        delegated_method = _find_method_declaration(
            service_owner_methods,
            invocation.member,
            len(invocation.arguments),
            argument_types=deps.infer_argument_types(
                repo_root=repo_root,
                controller_path=controller_path,
                arguments=invocation.arguments,
                controller_field_types=service_field_types,
                variable_types=_collect_method_variable_types(service_method),
                variable_initializers=_collect_method_variable_initializers(
                    service_method
                ),
                import_map=service_import_map,
                package_name=service_package_name,
                owner_methods=service_owner_methods,
                active_same_class_methods=active_service_methods,
            ),
        )
        if delegated_method is None or delegated_method is service_method:
            continue
        return infer_service_method_payload_type(
            repo_root=repo_root,
            controller_path=controller_path,
            service_method=delegated_method,
            service_owner_methods=service_owner_methods,
            service_field_types=service_field_types,
            service_import_map=service_import_map,
            service_package_name=service_package_name,
            deps=deps,
            active_service_methods=active_service_methods,
        )
    return None


def collect_service_data_assignment_types(
    *,
    repo_root: Path,
    controller_path: Path,
    service_method: javalang.tree.MethodDeclaration,
    service_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    assignment_kind: str,
    deps: ServiceInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> list[str]:
    inferred_types: list[str] = []
    for _, invocation in service_method.filter(javalang.tree.MethodInvocation):
        if assignment_kind == "set_data":
            if invocation.member != "setData" or not invocation.arguments:
                continue
            expression = invocation.arguments[0]
        elif invocation.member == "putAll" and invocation.arguments:
            qualifier = invocation.qualifier or ""
            qualifier_type = variable_types.get(qualifier)
            if qualifier_type not in {
                "Map<String, Object>",
                "Map<String, Map<String, Object>>",
            }:
                continue
            expression = invocation.arguments[0]
        else:
            if invocation.member != "put" or len(invocation.arguments) < 2:
                continue
            if not deps.is_data_list_expression(invocation.arguments[0]):
                continue
            expression = invocation.arguments[1]
        inferred_type = deps.infer_expression_data_type(
            repo_root=repo_root,
            controller_path=controller_path,
            expression=expression,
            controller_field_types=service_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
        if inferred_type in {
            None,
            "ArrayNode",
            "Map<String, Object>",
            "Map<String, Map<String, Object>>",
        } and isinstance(expression, javalang.tree.MemberReference):
            inferred_type = deps.infer_local_data_structure_type(
                repo_root=repo_root,
                controller_path=controller_path,
                method=service_method,
                variable_name=expression.member,
                view_name_hint=(
                    f"{controller_path.stem}_{service_method.name}_{expression.member}"
                ),
                variable_types=variable_types,
                variable_initializers=variable_initializers,
                controller_field_types=service_field_types,
                import_map=import_map,
                package_name=package_name,
                owner_methods=owner_methods,
                active_same_class_methods=active_same_class_methods,
            )
        if assignment_kind == "data_list" and invocation.member == "putAll":
            inferred_type = deps.unwrap_generated_view_data_list_type(inferred_type)
        if inferred_type is not None:
            inferred_types.append(inferred_type)
    return inferred_types


def is_data_list_expression(expression: object) -> bool:
    if isinstance(expression, javalang.tree.MemberReference):
        return cast("str", expression.member) == "DATA_LIST"
    return False
