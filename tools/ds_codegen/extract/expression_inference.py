"""Expression-oriented return and data type inference."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import javalang

from ds_codegen.extract.inference_support import (
    _extract_method_invocation_with_qualifier,
    _is_weak_inferred_type,
)
from ds_codegen.extract.type_lookup import (
    _infer_imported_type_method_return_type,
    _infer_instance_method_return_type,
    _java_generic_base_type,
    _java_generic_inner_types,
    _render_type,
)
from ds_codegen.java_literals import infer_java_literal_type

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


@dataclass(frozen=True)
class ExpressionInferenceDeps:
    infer_argument_types: Callable[..., list[str | None]]
    infer_service_invocation_payload_type: Callable[..., str | None]
    infer_same_class_method_payload_type: Callable[..., str | None]
    infer_success_like_invocation_payload_type: Callable[..., str | None]
    infer_return_data_list_payload_type: Callable[..., str | None]
    infer_class_creator_return_type: Callable[..., str | None]
    infer_selector_chain_data_type: Callable[..., str | None]
    unwrap_result_like_type: Callable[[str], str | None]


def infer_expression_return_type(
    *,
    repo_root: Path,
    controller_path: Path,
    expression: object | None,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    deps: ExpressionInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    if expression is None:
        return "Void"
    extracted_invocation = _extract_method_invocation_with_qualifier(expression)
    if extracted_invocation is not None:
        invocation, qualifier_override = extracted_invocation
        return infer_method_invocation_return_type(
            repo_root=repo_root,
            controller_path=controller_path,
            invocation=invocation,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            deps=deps,
            qualifier_override=qualifier_override,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
    if isinstance(expression, javalang.tree.MemberReference):
        variable_type = variable_types.get(expression.member)
        if variable_type is None:
            return None
        unwrapped_variable_type = deps.unwrap_result_like_type(variable_type)
        if unwrapped_variable_type is None or _is_weak_inferred_type(
            unwrapped_variable_type
        ):
            return None
        return unwrapped_variable_type
    if isinstance(expression, javalang.tree.ClassCreator):
        return deps.infer_class_creator_return_type(
            repo_root=repo_root,
            controller_path=controller_path,
            class_creator=expression,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
    if isinstance(expression, javalang.tree.Cast):
        selector_return_type = deps.infer_selector_chain_data_type(
            repo_root=repo_root,
            controller_path=controller_path,
            base_java_type=_render_type(expression.type),
            selectors=expression.selectors or [],
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
        if selector_return_type is not None:
            return selector_return_type
        return _render_type(expression.type)
    if isinstance(expression, javalang.tree.ClassReference):
        return f"Class<{_render_type(expression.type)}>"
    if isinstance(expression, javalang.tree.BinaryOperation):
        return _infer_binary_operation_type(
            repo_root=repo_root,
            controller_path=controller_path,
            expression=expression,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
    if isinstance(expression, javalang.tree.Literal):
        return infer_literal_type(expression)
    return None


def infer_method_invocation_return_type(
    *,
    repo_root: Path,
    controller_path: Path,
    invocation: javalang.tree.MethodInvocation,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    deps: ExpressionInferenceDeps,
    qualifier_override: str | None = None,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    qualifier = (
        qualifier_override
        if qualifier_override is not None
        else invocation.qualifier or ""
    )
    response_entity_body_type = _infer_response_entity_body_payload_type(
        repo_root=repo_root,
        controller_path=controller_path,
        invocation=invocation,
        controller_field_types=controller_field_types,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        import_map=import_map,
        package_name=package_name,
        deps=deps,
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )
    if response_entity_body_type is not None:
        return response_entity_body_type
    if invocation.member in {"error", "errorWithArgs"} and qualifier in {
        "",
        "Result",
        "this",
    }:
        return None
    if invocation.member in {"success", "getResult"} and qualifier in {
        "",
        "Result",
        "this",
    }:
        payload_type = deps.infer_success_like_invocation_payload_type(
            repo_root=repo_root,
            controller_path=controller_path,
            invocation=invocation,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
        return _apply_invocation_selectors_return_type(
            base_return_type=payload_type,
            invocation=invocation,
            repo_root=repo_root,
            controller_path=controller_path,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
    if invocation.member == "returnDataList" and qualifier in {"", "this"}:
        payload_type = deps.infer_return_data_list_payload_type(
            repo_root=repo_root,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            invocation=invocation,
            controller_path=controller_path,
            import_map=import_map,
            package_name=package_name,
        )
        return _apply_invocation_selectors_return_type(
            base_return_type=payload_type,
            invocation=invocation,
            repo_root=repo_root,
            controller_path=controller_path,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
    if qualifier in controller_field_types:
        payload_type = deps.infer_service_invocation_payload_type(
            repo_root=repo_root,
            service_field_type=controller_field_types[qualifier],
            service_method_name=invocation.member,
            service_method_arity=len(invocation.arguments),
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
                active_same_class_methods=active_same_class_methods,
            ),
            import_map=import_map,
            package_name=package_name,
        )
        return _apply_invocation_selectors_return_type(
            base_return_type=payload_type,
            invocation=invocation,
            repo_root=repo_root,
            controller_path=controller_path,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
    if qualifier in variable_types:
        instance_method_return_type = _infer_instance_method_return_type(
            repo_root=repo_root,
            instance_java_type=variable_types[qualifier],
            method_name=invocation.member,
            method_arity=len(invocation.arguments),
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
                active_same_class_methods=active_same_class_methods,
            ),
            import_map=import_map,
            package_name=package_name,
        )
        if instance_method_return_type is None:
            instance_method_return_type = _infer_common_jdk_instance_method_return_type(
                instance_java_type=variable_types[qualifier],
                method_name=invocation.member,
                method_arity=len(invocation.arguments),
            )
        if instance_method_return_type is not None:
            return _apply_invocation_selectors_return_type(
                base_return_type=instance_method_return_type,
                invocation=invocation,
                repo_root=repo_root,
                controller_path=controller_path,
                controller_field_types=controller_field_types,
                variable_types=variable_types,
                variable_initializers=variable_initializers,
                import_map=import_map,
                package_name=package_name,
                deps=deps,
                owner_methods=owner_methods,
                active_same_class_methods=active_same_class_methods,
            )
    if qualifier and "." not in qualifier:
        imported_type_return_type = _infer_imported_type_method_return_type(
            repo_root=repo_root,
            type_name=qualifier,
            method_name=invocation.member,
            method_arity=len(invocation.arguments),
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
                active_same_class_methods=active_same_class_methods,
            ),
            import_map=import_map,
            package_name=package_name,
        )
        if imported_type_return_type is not None:
            return _apply_invocation_selectors_return_type(
                base_return_type=imported_type_return_type,
                invocation=invocation,
                repo_root=repo_root,
                controller_path=controller_path,
                controller_field_types=controller_field_types,
                variable_types=variable_types,
                variable_initializers=variable_initializers,
                import_map=import_map,
                package_name=package_name,
                deps=deps,
                owner_methods=owner_methods,
                active_same_class_methods=active_same_class_methods,
            )
    if qualifier in {"", "this"} and owner_methods is not None:
        payload_type = deps.infer_same_class_method_payload_type(
            repo_root=repo_root,
            controller_path=controller_path,
            owner_methods=owner_methods,
            method_name=invocation.member,
            method_arity=len(invocation.arguments),
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
                active_same_class_methods=active_same_class_methods,
            ),
            controller_field_types=controller_field_types,
            import_map=import_map,
            package_name=package_name,
            active_same_class_methods=active_same_class_methods,
        )
        return _apply_invocation_selectors_return_type(
            base_return_type=payload_type,
            invocation=invocation,
            repo_root=repo_root,
            controller_path=controller_path,
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


def _apply_invocation_selectors_return_type(
    *,
    base_return_type: str | None,
    invocation: javalang.tree.MethodInvocation,
    repo_root: Path,
    controller_path: Path,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    deps: ExpressionInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    if base_return_type is None or not invocation.selectors:
        return base_return_type
    selector_return_type = deps.infer_selector_chain_data_type(
        repo_root=repo_root,
        controller_path=controller_path,
        base_java_type=base_return_type,
        selectors=invocation.selectors,
        controller_field_types=controller_field_types,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        import_map=import_map,
        package_name=package_name,
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )
    return selector_return_type or base_return_type


def _infer_common_jdk_instance_method_return_type(
    *,
    instance_java_type: str,
    method_name: str,
    method_arity: int,
) -> str | None:
    if method_arity != 0:
        return None
    base_type = _java_generic_base_type(instance_java_type)
    inner_types = _java_generic_inner_types(instance_java_type)
    element_type = inner_types[0] if inner_types else "Object"
    if method_name == "stream" and base_type in {"Collection", "List", "Set"}:
        return f"Stream<{element_type}>"
    if method_name == "distinct" and base_type == "Stream":
        return instance_java_type
    return None


def infer_expression_data_type(
    *,
    repo_root: Path,
    controller_path: Path,
    expression: object,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    deps: ExpressionInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    if isinstance(expression, javalang.tree.MemberReference):
        variable_type = variable_types.get(expression.member)
        if variable_type is not None:
            return variable_type
        if expression.qualifier == "Collections" and expression.member == "emptyList":
            return "List<Object>"
        return None
    extracted_invocation = _extract_method_invocation_with_qualifier(expression)
    if extracted_invocation is not None:
        invocation, qualifier_override = extracted_invocation
        if qualifier_override == "Collections" and invocation.member == "emptyList":
            return "List<Object>"
        return infer_expression_return_type(
            repo_root=repo_root,
            controller_path=controller_path,
            expression=expression,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
    if isinstance(expression, javalang.tree.ClassCreator):
        if _render_type(class_creator := expression.type) == "ArrayList":
            return "List<Object>"
        return _render_type(class_creator)
    if isinstance(expression, javalang.tree.Cast):
        selector_return_type = deps.infer_selector_chain_data_type(
            repo_root=repo_root,
            controller_path=controller_path,
            base_java_type=_render_type(expression.type),
            selectors=expression.selectors or [],
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
        if selector_return_type is not None:
            return selector_return_type
        return _render_type(expression.type)
    if isinstance(expression, javalang.tree.ClassReference):
        return f"Class<{_render_type(expression.type)}>"
    if isinstance(expression, javalang.tree.BinaryOperation):
        return _infer_binary_operation_type(
            repo_root=repo_root,
            controller_path=controller_path,
            expression=expression,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
    if isinstance(expression, javalang.tree.Literal):
        return infer_literal_type(expression)
    return None


def _infer_response_entity_body_payload_type(
    *,
    repo_root: Path,
    controller_path: Path,
    invocation: javalang.tree.MethodInvocation,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    deps: ExpressionInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    qualifier = invocation.qualifier or ""
    if qualifier != "ResponseEntity":
        return None
    for selector in invocation.selectors or []:
        if not isinstance(selector, javalang.tree.MethodInvocation):
            continue
        if selector.member != "body" or not selector.arguments:
            continue
        return infer_expression_data_type(
            repo_root=repo_root,
            controller_path=controller_path,
            expression=selector.arguments[0],
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


def _infer_binary_operation_type(
    *,
    repo_root: Path,
    controller_path: Path,
    expression: javalang.tree.BinaryOperation,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    deps: ExpressionInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    if expression.operator in {"&&", "||", "==", "!=", ">", "<", ">=", "<="}:
        return "Boolean"
    left_type = infer_expression_data_type(
        repo_root=repo_root,
        controller_path=controller_path,
        expression=expression.operandl,
        controller_field_types=controller_field_types,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        import_map=import_map,
        package_name=package_name,
        deps=deps,
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )
    right_type = infer_expression_data_type(
        repo_root=repo_root,
        controller_path=controller_path,
        expression=expression.operandr,
        controller_field_types=controller_field_types,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        import_map=import_map,
        package_name=package_name,
        deps=deps,
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )
    if expression.operator == "+" and "String" in {left_type, right_type}:
        return "String"
    return None


def infer_literal_type(literal: javalang.tree.Literal) -> str:
    return infer_java_literal_type(
        str(literal.value),
        prefix_operators=literal.prefix_operators,
    )
