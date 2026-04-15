"""Operation-level return inference extracted from the main extractor."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import javalang

from ds_codegen.extract.inference_support import (
    _collect_method_variable_initializers,
    _collect_method_variable_types,
    _extract_method_invocation_with_qualifier,
    _is_weak_inferred_type,
    _method_signature_key,
    _resolve_inferred_return_type,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


@dataclass(frozen=True)
class OperationInferenceDeps:
    # Keep the module focused on operation orchestration while lower-level
    # expression and local-structure inference continues to move out gradually.
    infer_local_data_structure_type: Callable[..., str | None]
    infer_local_variable_payload_type: Callable[..., str | None]
    infer_return_statement_type: Callable[..., str | None]
    infer_local_return_statement_payload_type: Callable[..., str | None]


def infer_operation_return_type(
    *,
    repo_root: Path,
    controller_path: Path,
    method: javalang.tree.MethodDeclaration,
    controller_field_types: dict[str, str],
    import_map: dict[str, str],
    package_name: str | None,
    deps: OperationInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    variable_types = _collect_method_variable_types(method)
    variable_initializers = _collect_method_variable_initializers(method)
    inferred_return_types: list[str] = []
    saw_success_like_void = False
    method_key = _method_signature_key(method)
    expression_active_methods = active_same_class_methods
    if method_key not in expression_active_methods:
        expression_active_methods = (*expression_active_methods, method_key)

    for _, return_statement in method.filter(javalang.tree.ReturnStatement):
        structured_inferred = infer_structured_return_statement_type(
            repo_root=repo_root,
            controller_path=controller_path,
            method=method,
            return_statement=return_statement,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            deps=deps,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        )
        if structured_inferred is not None:
            if structured_inferred == "Void":
                saw_success_like_void = True
            else:
                inferred_return_types.append(structured_inferred)
            continue
        inferred = deps.infer_return_statement_type(
            repo_root=repo_root,
            controller_path=controller_path,
            return_statement=return_statement,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            owner_methods=owner_methods,
            active_same_class_methods=expression_active_methods,
        )
        if inferred is None:
            inferred = deps.infer_local_return_statement_payload_type(
                repo_root=repo_root,
                controller_path=controller_path,
                method=method,
                return_statement=return_statement,
                controller_field_types=controller_field_types,
                variable_types=variable_types,
                variable_initializers=variable_initializers,
                import_map=import_map,
                package_name=package_name,
                owner_methods=owner_methods,
                active_same_class_methods=active_same_class_methods,
            )
        if inferred is None:
            continue
        if inferred == "Void":
            saw_success_like_void = True
            continue
        inferred_return_types.append(inferred)

    return _resolve_inferred_return_type(
        inferred_return_types,
        saw_success_like_void=saw_success_like_void,
    )


def infer_structured_return_statement_type(
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
    deps: OperationInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    extracted_invocation = _extract_method_invocation_with_qualifier(
        return_statement.expression,
    )
    if extracted_invocation is None:
        return None
    invocation, qualifier = extracted_invocation
    if invocation.member not in {"success", "getResult", "returnDataList"}:
        return None
    if qualifier not in {"", "Result", "this"}:
        return None
    if not invocation.arguments:
        return None
    target_expression = invocation.arguments[0]
    if invocation.member == "getResult":
        if len(invocation.arguments) < 2:
            return None
        target_expression = invocation.arguments[1]
    if not isinstance(target_expression, javalang.tree.MemberReference):
        return None
    payload_type = deps.infer_local_variable_payload_type(
        repo_root=repo_root,
        controller_path=controller_path,
        method=method,
        variable_name=target_expression.member,
        controller_field_types=controller_field_types,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        import_map=import_map,
        package_name=package_name,
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )
    if (
        payload_type is not None
        and payload_type != "Void"
        and not _is_weak_inferred_type(payload_type)
    ):
        return payload_type
    structured_type = deps.infer_local_data_structure_type(
        repo_root=repo_root,
        controller_path=controller_path,
        method=method,
        variable_name=target_expression.member,
        view_name_hint=(
            f"{controller_path.stem}_{method.name}_{target_expression.member}"
        ),
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        controller_field_types=controller_field_types,
        import_map=import_map,
        package_name=package_name,
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )
    if structured_type is not None:
        return structured_type
    if payload_type is not None and not _is_weak_inferred_type(payload_type):
        return payload_type
    return None
