"""Structured local variable shape inference."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import javalang

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


@dataclass(frozen=True)
class StructureInferenceDeps:
    infer_local_variable_payload_type: Callable[..., str | None]
    infer_expression_data_type: Callable[..., str | None]
    resolve_string_constant_value: Callable[..., str | None]
    register_generated_view_model: Callable[..., str]
    get_generated_view_model_fields: Callable[[str], list[tuple[str, str]] | None]
    is_weak_inferred_type: Callable[[str], bool]


def infer_local_data_structure_type(
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
    deps: StructureInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
    active_variables: tuple[str, ...] = (),
) -> str | None:
    if variable_name in active_variables:
        return None
    variable_type = variable_types.get(variable_name)
    if variable_type is None:
        return None
    nested_active_variables = (*active_variables, variable_name)
    if variable_type == "ArrayNode":
        return infer_local_collection_shape_type(
            repo_root=repo_root,
            controller_path=controller_path,
            method=method,
            variable_name=variable_name,
            view_name_hint=view_name_hint,
            collection_base_type="List",
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            controller_field_types=controller_field_types,
            import_map=import_map,
            package_name=package_name,
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
            active_variables=nested_active_variables,
        )
    if is_collection_like_java_type(variable_type):
        collection_base_type = normalized_collection_base_type(variable_type)
        if collection_base_type is not None:
            return infer_local_collection_shape_type(
                repo_root=repo_root,
                controller_path=controller_path,
                method=method,
                variable_name=variable_name,
                view_name_hint=view_name_hint,
                collection_base_type=collection_base_type,
                variable_types=variable_types,
                variable_initializers=variable_initializers,
                controller_field_types=controller_field_types,
                import_map=import_map,
                package_name=package_name,
                deps=deps,
                owner_methods=owner_methods,
                active_same_class_methods=active_same_class_methods,
                active_variables=nested_active_variables,
            )
    if variable_type in {"Map<String, Object>", "Map<String, Map<String, Object>>"}:
        return _infer_local_map_shape_type(
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
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
            active_variables=nested_active_variables,
        )
    if variable_type == "ObjectNode":
        return _infer_local_object_node_type(
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
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
            active_variables=nested_active_variables,
        )
    return None


def _infer_local_map_shape_type(
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
    deps: StructureInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
    active_variables: tuple[str, ...] = (),
) -> str | None:
    fixed_fields: list[tuple[str, str]] = []
    dynamic_value_types: list[str] = []
    for _, invocation in method.filter(javalang.tree.MethodInvocation):
        qualifier = invocation.qualifier or ""
        if qualifier != variable_name:
            continue
        if invocation.member == "putAll" and invocation.arguments:
            nested_expression = invocation.arguments[0]
            if isinstance(nested_expression, javalang.tree.MemberReference):
                nested_type = infer_local_data_structure_type(
                    repo_root=repo_root,
                    controller_path=controller_path,
                    method=method,
                    variable_name=nested_expression.member,
                    view_name_hint=f"{view_name_hint}Merged",
                    variable_types=variable_types,
                    variable_initializers=variable_initializers,
                    controller_field_types=controller_field_types,
                    import_map=import_map,
                    package_name=package_name,
                    deps=deps,
                    owner_methods=owner_methods,
                    active_same_class_methods=active_same_class_methods,
                    active_variables=active_variables,
                )
                nested_fields = (
                    deps.get_generated_view_model_fields(nested_type)
                    if nested_type is not None
                    else None
                )
                if nested_fields is not None:
                    fixed_fields.extend(nested_fields)
            continue
        if invocation.member != "put" or len(invocation.arguments) < 2:
            continue
        key_name = deps.resolve_string_constant_value(
            repo_root=repo_root,
            expression=invocation.arguments[0],
            import_map=import_map,
            package_name=package_name,
            controller_path=controller_path,
        )
        value_java_type = infer_structured_expression_data_type(
            repo_root=repo_root,
            controller_path=controller_path,
            method=method,
            expression=invocation.arguments[1],
            view_name_hint=f"{view_name_hint}Value",
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            controller_field_types=controller_field_types,
            import_map=import_map,
            package_name=package_name,
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
            active_variables=active_variables,
        )
        if value_java_type is None:
            continue
        if key_name is None:
            dynamic_value_types.append(value_java_type)
            continue
        fixed_fields.append((key_name, value_java_type))
    if fixed_fields:
        deduped_fields: list[tuple[str, str]] = []
        seen_field_names: set[str] = set()
        for field_name, field_type in fixed_fields:
            if field_name in seen_field_names:
                continue
            seen_field_names.add(field_name)
            deduped_fields.append((field_name, field_type))
        return deps.register_generated_view_model(
            base_name=view_name_hint,
            fields=deduped_fields,
        )
    unique_dynamic_value_types = list(dict.fromkeys(dynamic_value_types))
    if len(unique_dynamic_value_types) == 1:
        return f"Map<String, {unique_dynamic_value_types[0]}>"
    return None


def _infer_local_object_node_type(
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
    deps: StructureInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
    active_variables: tuple[str, ...] = (),
) -> str | None:
    fields: list[tuple[str, str]] = []
    for _, invocation in method.filter(javalang.tree.MethodInvocation):
        qualifier = invocation.qualifier or ""
        if qualifier != variable_name:
            continue
        if invocation.member not in {"put", "set"} or len(invocation.arguments) < 2:
            continue
        key_name = deps.resolve_string_constant_value(
            repo_root=repo_root,
            expression=invocation.arguments[0],
            import_map=import_map,
            package_name=package_name,
            controller_path=controller_path,
        )
        if key_name is None:
            continue
        value_java_type = infer_structured_expression_data_type(
            repo_root=repo_root,
            controller_path=controller_path,
            method=method,
            expression=invocation.arguments[1],
            view_name_hint=f"{view_name_hint}_{key_name}",
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            controller_field_types=controller_field_types,
            import_map=import_map,
            package_name=package_name,
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
            active_variables=active_variables,
        )
        if value_java_type is None:
            continue
        fields.append((key_name, value_java_type))
    if not fields:
        return None
    return deps.register_generated_view_model(
        base_name=view_name_hint,
        fields=fields,
    )


def infer_local_collection_shape_type(
    *,
    repo_root: Path,
    controller_path: Path,
    method: javalang.tree.MethodDeclaration,
    variable_name: str,
    view_name_hint: str,
    collection_base_type: str,
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    controller_field_types: dict[str, str],
    import_map: dict[str, str],
    package_name: str | None,
    deps: StructureInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
    active_variables: tuple[str, ...] = (),
) -> str | None:
    item_types: list[str] = []
    for _, invocation in method.filter(javalang.tree.MethodInvocation):
        qualifier = invocation.qualifier or ""
        if qualifier != variable_name:
            continue
        if invocation.member != "add" or not invocation.arguments:
            continue
        item_java_type = infer_structured_expression_data_type(
            repo_root=repo_root,
            controller_path=controller_path,
            method=method,
            expression=invocation.arguments[0],
            view_name_hint=f"{view_name_hint}Item",
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            controller_field_types=controller_field_types,
            import_map=import_map,
            package_name=package_name,
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
            active_variables=active_variables,
        )
        if item_java_type is not None:
            item_types.append(item_java_type)
    unique_item_types = list(dict.fromkeys(item_types))
    if len(unique_item_types) == 1:
        return f"{collection_base_type}<{unique_item_types[0]}>"
    return None


def is_collection_like_java_type(java_type: str) -> bool:
    return java_type.startswith(
        ("List<", "ArrayList<", "LinkedList<", "Set<", "Collection<")
    )


def normalized_collection_base_type(java_type: str) -> str | None:
    if java_type.startswith(("List<", "ArrayList<", "LinkedList<")):
        return "List"
    if java_type.startswith("Set<"):
        return "Set"
    if java_type.startswith("Collection<"):
        return "Collection"
    return None


def infer_structured_expression_data_type(
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
    deps: StructureInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
    active_variables: tuple[str, ...] = (),
) -> str | None:
    if isinstance(expression, javalang.tree.MemberReference):
        structured_variable_type = infer_local_data_structure_type(
            repo_root=repo_root,
            controller_path=controller_path,
            method=method,
            variable_name=expression.member,
            view_name_hint=view_name_hint,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            controller_field_types=controller_field_types,
            import_map=import_map,
            package_name=package_name,
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
            active_variables=active_variables,
        )
        if structured_variable_type is not None:
            return structured_variable_type
        local_payload_type = deps.infer_local_variable_payload_type(
            repo_root=repo_root,
            controller_path=controller_path,
            method=method,
            variable_name=expression.member,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
        if local_payload_type is not None and not deps.is_weak_inferred_type(
            local_payload_type
        ):
            return local_payload_type
    return deps.infer_expression_data_type(
        repo_root=repo_root,
        controller_path=controller_path,
        expression=expression,
        controller_field_types=controller_field_types,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        import_map=import_map,
        package_name=package_name,
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )
