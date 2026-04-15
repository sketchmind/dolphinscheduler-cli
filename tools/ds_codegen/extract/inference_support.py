"""Common helpers shared by return-inference entrypoints."""

from __future__ import annotations

import javalang

from ds_codegen.extract.type_lookup import _render_type


def _collect_method_variable_types(
    method: javalang.tree.MethodDeclaration,
) -> dict[str, str]:
    variable_types: dict[str, str] = {}
    for parameter in method.parameters:
        variable_types[parameter.name] = _render_type(parameter.type)
    for _, for_statement in method.filter(javalang.tree.ForStatement):
        control = for_statement.control
        if not isinstance(control, javalang.tree.EnhancedForControl):
            continue
        java_type = _render_type(control.var.type)
        for declarator in control.var.declarators:
            variable_types[declarator.name] = java_type
    for _, local_declaration in method.filter(javalang.tree.LocalVariableDeclaration):
        java_type = _render_type(local_declaration.type)
        for declarator in local_declaration.declarators:
            variable_types[declarator.name] = java_type
    return variable_types


def _collect_method_variable_initializers(
    method: javalang.tree.MethodDeclaration,
) -> dict[str, object]:
    variable_initializers: dict[str, object] = {}
    for _, local_declaration in method.filter(javalang.tree.LocalVariableDeclaration):
        for declarator in local_declaration.declarators:
            if declarator.initializer is not None:
                variable_initializers[declarator.name] = declarator.initializer
    return variable_initializers


def _collect_class_field_types(
    type_declaration: javalang.tree.TypeDeclaration,
) -> dict[str, str]:
    field_types: dict[str, str] = {}
    for field in getattr(type_declaration, "fields", []):
        java_type = _render_type(field.type)
        for declarator in field.declarators:
            field_types[declarator.name] = java_type
    return field_types


def _resolve_inferred_return_type(
    inferred_return_types: list[str],
    *,
    saw_success_like_void: bool,
) -> str | None:
    unique_inferred_return_types = list(dict.fromkeys(inferred_return_types))
    if unique_inferred_return_types:
        specific_return_types = [
            inferred_return_type
            for inferred_return_type in unique_inferred_return_types
            if not _is_weak_inferred_type(inferred_return_type)
        ]
        if specific_return_types:
            unique_inferred_return_types = list(dict.fromkeys(specific_return_types))
    if not unique_inferred_return_types:
        return "Void" if saw_success_like_void else None
    if len(unique_inferred_return_types) == 1:
        inferred_return_type = unique_inferred_return_types[0]
        if saw_success_like_void:
            return f"Optional<{inferred_return_type}>"
        return inferred_return_type
    return "Any"


def _is_weak_inferred_type(java_type: str) -> bool:
    return java_type in {
        "Any",
        "Object",
        "List<Object>",
        "Map<String, Object>",
        "Set<Object>",
        "Collection<Object>",
        "Optional<Object>",
    }


def _extract_method_invocation_with_qualifier(
    expression: object | None,
) -> tuple[javalang.tree.MethodInvocation, str] | None:
    if isinstance(expression, javalang.tree.MethodInvocation):
        return expression, expression.qualifier or ""
    if not isinstance(expression, javalang.tree.This):
        return None
    selectors = expression.selectors or []
    if not selectors:
        return None
    last_selector = selectors[-1]
    if not isinstance(last_selector, javalang.tree.MethodInvocation):
        return None
    qualifier_parts: list[str] = []
    for selector in selectors[:-1]:
        if not isinstance(selector, javalang.tree.MemberReference):
            return None
        qualifier_parts.append(selector.member)
    return last_selector, ".".join(qualifier_parts)


def _method_signature_key(
    method: javalang.tree.MethodDeclaration,
) -> tuple[str, int]:
    parameter_types = ",".join(
        _render_type(parameter.type) for parameter in method.parameters
    )
    return (f"{method.name}({parameter_types})", len(method.parameters))
