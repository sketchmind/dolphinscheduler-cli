"""Local-variable-oriented payload inference."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import javalang

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


@dataclass(frozen=True)
class LocalInferenceDeps:
    infer_expression_return_type: Callable[..., str | None]
    infer_expression_data_type: Callable[..., str | None]
    infer_local_data_structure_type: Callable[..., str | None]
    collect_method_variable_types: Callable[
        [javalang.tree.MethodDeclaration],
        dict[str, str],
    ]
    collect_method_variable_initializers: Callable[
        [javalang.tree.MethodDeclaration],
        dict[str, object],
    ]
    infer_argument_types: Callable[..., list[str | None]]
    find_method_declaration: Callable[..., javalang.tree.MethodDeclaration | None]
    method_signature_key: Callable[
        [javalang.tree.MethodDeclaration],
        tuple[str, int],
    ]
    resolve_inferred_return_type: Callable[..., str | None]
    is_weak_inferred_type: Callable[[str], bool]
    is_collection_like_java_type: Callable[[str], bool]
    is_data_list_expression: Callable[[object], bool]
    unwrap_generated_view_data_list_type: Callable[[str | None], str | None]
    render_reference_name: Callable[[javalang.tree.ReferenceType], str]
    resolve_referenced_import_path: Callable[..., str | None]
    type_extends_result: Callable[[Path, str], bool]


def infer_local_return_statement_payload_type(
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
    deps: LocalInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    expression = return_statement.expression
    if isinstance(expression, javalang.tree.MemberReference):
        return infer_local_variable_payload_type(
            repo_root=repo_root,
            controller_path=controller_path,
            method=method,
            variable_name=expression.member,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
    if isinstance(expression, javalang.tree.MethodInvocation):
        qualifier = expression.qualifier or ""
        if (
            expression.member == "returnDataList"
            and qualifier in {"", "this"}
            and expression.arguments
            and isinstance(expression.arguments[0], javalang.tree.MemberReference)
        ):
            return infer_local_variable_payload_type(
                repo_root=repo_root,
                controller_path=controller_path,
                method=method,
                variable_name=expression.arguments[0].member,
                controller_field_types=controller_field_types,
                variable_types=variable_types,
                variable_initializers=variable_initializers,
                import_map=import_map,
                package_name=package_name,
                deps=deps,
                owner_methods=owner_methods,
                active_same_class_methods=active_same_class_methods,
            )
    return None


def infer_local_variable_payload_type(
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
    deps: LocalInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    variable_type = variable_types.get(variable_name)
    if variable_type is None:
        return None
    initializer = variable_initializers.get(variable_name)
    initializer_payload_type: str | None = None
    if initializer is not None:
        initializer_payload_type = deps.infer_expression_return_type(
            repo_root=repo_root,
            controller_path=controller_path,
            expression=initializer,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )

    if _is_result_like_java_type(
        repo_root=repo_root,
        java_type=variable_type,
        import_map=import_map,
        package_name=package_name,
        deps=deps,
    ):
        assignment_types = _collect_local_variable_assignment_types(
            repo_root=repo_root,
            controller_path=controller_path,
            method=method,
            variable_name=variable_name,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            assignment_kind="set_data",
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
        inferred_assignment_type = deps.resolve_inferred_return_type(
            assignment_types,
            saw_success_like_void=False,
        )
        if inferred_assignment_type is not None:
            return inferred_assignment_type
        if initializer_payload_type is not None and not deps.is_weak_inferred_type(
            initializer_payload_type
        ):
            return initializer_payload_type
        if _creates_empty_result_like_initializer(
            repo_root=repo_root,
            initializer=initializer,
            import_map=import_map,
            package_name=package_name,
            deps=deps,
        ):
            return "Void"

    if (
        not variable_type.startswith("Map<")
        and not deps.is_collection_like_java_type(variable_type)
        and not deps.is_weak_inferred_type(variable_type)
    ):
        return variable_type

    if variable_type.startswith("Map<"):
        if _creates_empty_map_like_initializer(
            initializer,
            deps=deps,
        ) and _map_variable_only_carries_status_metadata(
            repo_root=repo_root,
            controller_path=controller_path,
            method=method,
            variable_name=variable_name,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        ):
            return "Void"
        structured_map_type = deps.infer_local_data_structure_type(
            repo_root=repo_root,
            controller_path=controller_path,
            method=method,
            variable_name=variable_name,
            view_name_hint=f"{controller_path.stem}_{method.name}_{variable_name}",
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            controller_field_types=controller_field_types,
            import_map=import_map,
            package_name=package_name,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
        if structured_map_type is not None:
            return structured_map_type
        assignment_types = _collect_local_variable_assignment_types(
            repo_root=repo_root,
            controller_path=controller_path,
            method=method,
            variable_name=variable_name,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            assignment_kind="data_list",
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
        inferred_assignment_type = deps.resolve_inferred_return_type(
            assignment_types,
            saw_success_like_void=False,
        )
        if inferred_assignment_type is not None:
            return inferred_assignment_type
        if initializer_payload_type is not None and not deps.is_weak_inferred_type(
            initializer_payload_type
        ):
            return initializer_payload_type
        if _creates_empty_map_like_initializer(initializer, deps=deps):
            return "Void"
    if deps.is_collection_like_java_type(variable_type):
        structured_collection_type = deps.infer_local_data_structure_type(
            repo_root=repo_root,
            controller_path=controller_path,
            method=method,
            variable_name=variable_name,
            view_name_hint=f"{controller_path.stem}_{method.name}_{variable_name}",
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            controller_field_types=controller_field_types,
            import_map=import_map,
            package_name=package_name,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
        if structured_collection_type is not None:
            return structured_collection_type
        if initializer_payload_type is not None and not deps.is_weak_inferred_type(
            initializer_payload_type
        ):
            return initializer_payload_type
    if initializer_payload_type is not None and not deps.is_weak_inferred_type(
        initializer_payload_type
    ):
        return initializer_payload_type
    return None


def _collect_local_variable_assignment_types(
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
    assignment_kind: str,
    deps: LocalInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> list[str]:
    method_key = deps.method_signature_key(method)
    if method_key in active_same_class_methods:
        return []
    nested_active_methods = (*active_same_class_methods, method_key)
    inferred_types: list[str] = []
    for _, invocation in method.filter(javalang.tree.MethodInvocation):
        qualifier = invocation.qualifier or ""
        if qualifier == variable_name:
            if assignment_kind == "set_data":
                if invocation.member != "setData" or not invocation.arguments:
                    continue
                expression = invocation.arguments[0]
            elif invocation.member == "putAll" and invocation.arguments:
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
                controller_field_types=controller_field_types,
                variable_types=variable_types,
                variable_initializers=variable_initializers,
                import_map=import_map,
                package_name=package_name,
                owner_methods=owner_methods,
                active_same_class_methods=nested_active_methods,
            )
            if assignment_kind == "data_list" and invocation.member == "putAll":
                inferred_type = (
                    deps.unwrap_generated_view_data_list_type(inferred_type)
                    or inferred_type
                )
            if inferred_type is not None:
                inferred_types.append(inferred_type)
            continue
        if owner_methods is None or qualifier not in {"", "this"}:
            continue
        helper_method = deps.find_method_declaration(
            owner_methods,
            invocation.member,
            len(invocation.arguments),
            argument_types=deps.infer_argument_types(
                repo_root=repo_root,
                controller_path=controller_path,
                arguments=invocation.arguments,
                controller_field_types=controller_field_types,
                variable_types=variable_types,
                variable_initializers=variable_initializers,
                import_map=import_map,
                package_name=package_name,
                owner_methods=owner_methods,
                active_same_class_methods=nested_active_methods,
            ),
        )
        if helper_method is None:
            continue
        argument_index: int | None = None
        for index, argument in enumerate(invocation.arguments):
            if (
                isinstance(argument, javalang.tree.MemberReference)
                and argument.member == variable_name
            ):
                argument_index = index
                break
        if argument_index is None or argument_index >= len(helper_method.parameters):
            continue
        inferred_types.extend(
            _collect_local_variable_assignment_types(
                repo_root=repo_root,
                controller_path=controller_path,
                method=helper_method,
                variable_name=helper_method.parameters[argument_index].name,
                controller_field_types=controller_field_types,
                variable_types=deps.collect_method_variable_types(helper_method),
                variable_initializers=deps.collect_method_variable_initializers(
                    helper_method
                ),
                import_map=import_map,
                package_name=package_name,
                assignment_kind=assignment_kind,
                deps=deps,
                owner_methods=owner_methods,
                active_same_class_methods=nested_active_methods,
            )
        )
    return inferred_types


def _map_variable_only_carries_status_metadata(
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
    deps: LocalInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> bool:
    method_key = deps.method_signature_key(method)
    if method_key in active_same_class_methods:
        return False
    nested_active_methods = (*active_same_class_methods, method_key)
    saw_assignment = False

    for _, invocation in method.filter(javalang.tree.MethodInvocation):
        qualifier = invocation.qualifier or ""
        if qualifier == variable_name:
            if invocation.member == "put" and len(invocation.arguments) >= 2:
                saw_assignment = True
                if not _is_status_metadata_expression(invocation.arguments[0]):
                    return False
                continue
            if invocation.member == "putAll" and invocation.arguments:
                return False
            continue

        if owner_methods is None or qualifier not in {"", "this"}:
            continue
        helper_method = deps.find_method_declaration(
            owner_methods,
            invocation.member,
            len(invocation.arguments),
            argument_types=deps.infer_argument_types(
                repo_root=repo_root,
                controller_path=controller_path,
                arguments=invocation.arguments,
                controller_field_types=controller_field_types,
                variable_types=variable_types,
                variable_initializers=variable_initializers,
                import_map=import_map,
                package_name=package_name,
                owner_methods=owner_methods,
                active_same_class_methods=nested_active_methods,
            ),
        )
        if helper_method is None:
            continue

        argument_index: int | None = None
        for index, argument in enumerate(invocation.arguments):
            if (
                isinstance(argument, javalang.tree.MemberReference)
                and argument.member == variable_name
            ):
                argument_index = index
                break
        if argument_index is None or argument_index >= len(helper_method.parameters):
            continue

        saw_assignment = True
        if not _map_variable_only_carries_status_metadata(
            repo_root=repo_root,
            controller_path=controller_path,
            method=helper_method,
            variable_name=helper_method.parameters[argument_index].name,
            controller_field_types=controller_field_types,
            variable_types=deps.collect_method_variable_types(helper_method),
            variable_initializers=deps.collect_method_variable_initializers(
                helper_method
            ),
            import_map=import_map,
            package_name=package_name,
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=nested_active_methods,
        ):
            return False

    return saw_assignment


def _is_status_metadata_expression(expression: object) -> bool:
    if isinstance(expression, javalang.tree.Literal):
        value = expression.value.strip("\"'")
        return value in {"code", "msg", "status"}
    if isinstance(expression, javalang.tree.MemberReference):
        return expression.member in {"CODE", "MSG", "STATUS"}
    return False


def _creates_empty_result_like_initializer(
    *,
    repo_root: Path,
    initializer: object | None,
    import_map: dict[str, str],
    package_name: str | None,
    deps: LocalInferenceDeps,
) -> bool:
    if not isinstance(initializer, javalang.tree.ClassCreator):
        return False
    created_type_name = deps.render_reference_name(initializer.type)
    if created_type_name == "Result":
        return True
    import_path = deps.resolve_referenced_import_path(
        repo_root,
        created_type_name,
        import_map,
        package_name,
    )
    if import_path is None:
        return False
    return deps.type_extends_result(repo_root, import_path)


def _creates_empty_map_like_initializer(
    initializer: object | None,
    *,
    deps: LocalInferenceDeps,
) -> bool:
    if not isinstance(initializer, javalang.tree.ClassCreator):
        return False
    return deps.render_reference_name(initializer.type) in {
        "HashMap",
        "LinkedHashMap",
        "Map",
        "TreeMap",
    }


def _is_result_like_java_type(
    *,
    repo_root: Path,
    java_type: str,
    import_map: dict[str, str],
    package_name: str | None,
    deps: LocalInferenceDeps,
) -> bool:
    generic_base_type = java_type.split("<", 1)[0]
    if generic_base_type == "Result":
        return True
    import_path = deps.resolve_referenced_import_path(
        repo_root,
        generic_base_type,
        import_map,
        package_name,
    )
    if import_path is None:
        return False
    return deps.type_extends_result(repo_root, import_path)
