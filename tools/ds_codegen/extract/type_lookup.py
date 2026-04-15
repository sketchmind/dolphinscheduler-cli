"""Shared Java type lookup helpers for contract extraction and inference."""

from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING, cast

import javalang

from ds_codegen.java_source import (
    JavaParseCache,
    LoadedTypeDeclaration,
    build_import_map,
    load_type_declaration,
    resolve_referenced_import_path,
    try_parse_java_compilation_unit,
)

if TYPE_CHECKING:
    from pathlib import Path


def _render_reference_name(type_node: javalang.tree.ReferenceType) -> str:
    base_name = cast("str", type_node.name)
    if type_node.sub_type is not None:
        return f"{base_name}.{_render_reference_name(type_node.sub_type)}"
    return base_name


def _render_type(type_node: object | None) -> str:
    if type_node is None:
        return "void"
    base: str
    if isinstance(type_node, javalang.tree.BasicType):
        base = cast("str", type_node.name)
    elif isinstance(type_node, javalang.tree.ReferenceType):
        base = cast("str", type_node.name)
        if type_node.arguments:
            arguments = ", ".join(
                _render_type_argument(argument) for argument in type_node.arguments
            )
            base = f"{base}<{arguments}>"
        if type_node.sub_type is not None:
            base = f"{base}.{_render_type(type_node.sub_type)}"
    else:
        base = str(type_node)

    dimensions = getattr(type_node, "dimensions", None) or []
    if dimensions:
        base = f"{base}{'[]' * len(dimensions)}"
    return base


def _render_type_argument(argument: object) -> str:
    if isinstance(argument, javalang.tree.TypeArgument):
        if argument.type is None:
            return "?"
        rendered_type = _render_type(argument.type)
        if argument.pattern_type is None:
            return rendered_type
        return f"{argument.pattern_type} {rendered_type}"
    return _render_type(argument)


def _find_method_declaration(
    methods: list[javalang.tree.MethodDeclaration],
    method_name: str,
    method_arity: int,
    argument_types: list[str | None] | None = None,
) -> javalang.tree.MethodDeclaration | None:
    exact_matches = [
        method
        for method in methods
        if method.name == method_name and len(method.parameters) == method_arity
    ]
    if len(exact_matches) == 1:
        return exact_matches[0]
    if exact_matches and argument_types is not None:
        best_matches: list[javalang.tree.MethodDeclaration] = []
        best_score: int | None = None
        for method in exact_matches:
            score = _score_parameter_argument_match(
                method.parameters,
                argument_types,
            )
            if score is None:
                continue
            if best_score is None or score > best_score:
                best_matches = [method]
                best_score = score
                continue
            if score == best_score:
                best_matches.append(method)
        if len(best_matches) == 1:
            return best_matches[0]
    name_matches = [method for method in methods if method.name == method_name]
    if len(name_matches) == 1:
        return name_matches[0]
    return None


def _score_method_argument_match(
    method: javalang.tree.MethodDeclaration,
    argument_types: list[str | None],
) -> int | None:
    return _score_parameter_argument_match(method.parameters, argument_types)


def _score_parameter_argument_match(
    parameters: list[javalang.tree.FormalParameter],
    argument_types: list[str | None],
) -> int | None:
    if len(parameters) != len(argument_types):
        return None
    score = 0
    for parameter, argument_type in zip(parameters, argument_types, strict=True):
        parameter_type = _render_type(parameter.type)
        if argument_type is None or argument_type in {"Any", "Object"}:
            continue
        if argument_type == "Void":
            continue
        normalized_parameter_type = _normalize_java_type_for_matching(parameter_type)
        normalized_argument_type = _normalize_java_type_for_matching(argument_type)
        if normalized_parameter_type == normalized_argument_type:
            score += 2
            continue
        parameter_base_type = _java_generic_base_type(normalized_parameter_type)
        argument_base_type = _java_generic_base_type(normalized_argument_type)
        if parameter_base_type == argument_base_type:
            score += 1
            continue
        return None
    return score


def _normalize_java_type_for_matching(java_type: str) -> str:
    primitive_aliases = {
        "boolean": "Boolean",
        "byte": "Byte",
        "double": "Double",
        "float": "Float",
        "int": "Integer",
        "long": "Long",
        "short": "Short",
        "void": "Void",
    }
    if java_type.endswith("[]"):
        return f"{_normalize_java_type_for_matching(java_type[:-2])}[]"
    base_type = _java_generic_base_type(java_type)
    if "<" not in java_type:
        return primitive_aliases.get(base_type, java_type)
    inner_types = ", ".join(
        _normalize_java_type_for_matching(inner_type)
        for inner_type in _java_generic_inner_types(java_type)
    )
    normalized_base = primitive_aliases.get(base_type, base_type)
    return f"{normalized_base}<{inner_types}>"


def _java_generic_base_type(java_type: str) -> str:
    return java_type.split("<", 1)[0]


def _java_generic_inner_types(java_type: str) -> list[str]:
    if "<" not in java_type or not java_type.endswith(">"):
        return []
    inner = java_type.split("<", 1)[1][:-1]
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for char in inner:
        if char == "<":
            depth += 1
            current.append(char)
            continue
        if char == ">":
            depth -= 1
            current.append(char)
            continue
        if char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    if current:
        parts.append("".join(current).strip())
    return parts


@cache
def _load_cached_type_declaration(
    repo_root: Path,
    import_path: str,
) -> LoadedTypeDeclaration | None:
    parse_cache: JavaParseCache = {}
    return _try_load_type_declaration(repo_root, import_path, parse_cache)


def _try_load_type_declaration(
    repo_root: Path,
    import_path: str,
    parse_cache: JavaParseCache,
) -> LoadedTypeDeclaration | None:
    try:
        return load_type_declaration(repo_root, import_path, parse_cache)
    except (
        AttributeError,
        IndexError,
        OSError,
        TypeError,
        UnicodeDecodeError,
        javalang.parser.JavaSyntaxError,
        javalang.tokenizer.LexerError,
    ):
        return None


@cache
def _resolve_service_impl_import_path(
    repo_root: Path,
    service_import_path: str,
) -> str | None:
    return dict(_build_service_impl_index(repo_root)).get(service_import_path)


@cache
def _build_service_impl_index(repo_root: Path) -> tuple[tuple[str, str], ...]:
    impl_root = (
        repo_root
        / "references/dolphinscheduler/dolphinscheduler-api/src/main/java/"
        / "org/apache/dolphinscheduler/api/service/impl"
    )
    service_impl_index: dict[str, str] = {}
    for impl_path in sorted(impl_root.glob("*.java")):
        try:
            source = impl_path.read_text()
        except (OSError, UnicodeDecodeError):
            continue
        compilation_unit = try_parse_java_compilation_unit(source)
        if compilation_unit is None or not compilation_unit.types:
            continue
        type_declaration = compilation_unit.types[0]
        if not isinstance(type_declaration, javalang.tree.ClassDeclaration):
            continue
        package_name = (
            compilation_unit.package.name if compilation_unit.package else None
        )
        if package_name is None:
            continue
        import_map = build_import_map(compilation_unit)
        impl_import_path = f"{package_name}.{type_declaration.name}"
        for interface_type in type_declaration.implements or []:
            interface_import_path = resolve_referenced_import_path(
                repo_root,
                _render_reference_name(interface_type),
                import_map,
                package_name,
            )
            if interface_import_path is None:
                continue
            service_impl_index.setdefault(interface_import_path, impl_import_path)
    return tuple(sorted(service_impl_index.items()))


def _load_java_type_context(
    *,
    repo_root: Path,
    java_type: str,
    import_map: dict[str, str],
    package_name: str | None,
) -> LoadedTypeDeclaration | None:
    type_import_path = resolve_referenced_import_path(
        repo_root,
        _java_generic_base_type(java_type),
        import_map,
        package_name,
    )
    if type_import_path is None:
        return None
    return _load_cached_type_declaration(repo_root, type_import_path)


def _infer_imported_type_method_return_type(
    *,
    repo_root: Path,
    type_name: str,
    method_name: str,
    method_arity: int,
    argument_types: list[str | None] | None,
    import_map: dict[str, str],
    package_name: str | None,
) -> str | None:
    loaded_type = _load_java_type_context(
        repo_root=repo_root,
        java_type=type_name,
        import_map=import_map,
        package_name=package_name,
    )
    if loaded_type is None:
        return None
    _, type_declaration, type_import_map, type_package_name = loaded_type
    method_declaration = _find_method_declaration(
        type_declaration.methods,
        method_name,
        method_arity,
        argument_types=argument_types,
    )
    if method_declaration is not None:
        return _render_method_return_type(
            method_declaration,
            argument_types=argument_types,
        )
    return _infer_accessor_field_type(
        repo_root=repo_root,
        type_declaration=type_declaration,
        method_name=method_name,
        method_arity=method_arity,
        import_map=type_import_map,
        package_name=type_package_name,
    )


def _infer_instance_method_return_type(
    *,
    repo_root: Path,
    instance_java_type: str,
    method_name: str,
    method_arity: int,
    argument_types: list[str | None] | None,
    import_map: dict[str, str],
    package_name: str | None,
) -> str | None:
    loaded_type = _load_java_type_context(
        repo_root=repo_root,
        java_type=instance_java_type,
        import_map=import_map,
        package_name=package_name,
    )
    if loaded_type is None:
        return None
    _, type_declaration, type_import_map, type_package_name = loaded_type
    receiver_substitutions = _infer_receiver_type_parameter_substitutions(
        instance_java_type=instance_java_type,
        type_declaration=type_declaration,
    )
    method_declaration = _find_method_declaration(
        type_declaration.methods,
        method_name,
        method_arity,
        argument_types=argument_types,
    )
    if method_declaration is not None:
        return _render_instance_method_return_type(
            method_declaration=method_declaration,
            argument_types=argument_types,
            receiver_substitutions=receiver_substitutions,
        )
    return _infer_accessor_field_type(
        repo_root=repo_root,
        type_declaration=type_declaration,
        method_name=method_name,
        method_arity=method_arity,
        import_map=type_import_map,
        package_name=type_package_name,
    )


def _render_method_return_type(
    method_declaration: javalang.tree.MethodDeclaration,
    *,
    argument_types: list[str | None] | None,
) -> str:
    return_type = _render_type(method_declaration.return_type)
    if not method_declaration.type_parameters or argument_types is None:
        return return_type
    substitutions = _infer_method_type_parameter_substitutions(
        method_declaration,
        argument_types,
    )
    if not substitutions:
        return return_type
    return _substitute_java_type_parameters(return_type, substitutions)


def _render_instance_method_return_type(
    *,
    method_declaration: javalang.tree.MethodDeclaration,
    argument_types: list[str | None] | None,
    receiver_substitutions: dict[str, str],
) -> str:
    return_type = _render_method_return_type(
        method_declaration,
        argument_types=argument_types,
    )
    if not receiver_substitutions:
        return return_type
    return _substitute_java_type_parameters(return_type, receiver_substitutions)


def _infer_receiver_type_parameter_substitutions(
    *,
    instance_java_type: str,
    type_declaration: javalang.tree.TypeDeclaration,
) -> dict[str, str]:
    type_parameter_names = [
        type_parameter.name
        for type_parameter in getattr(type_declaration, "type_parameters", None) or []
    ]
    if not type_parameter_names:
        return {}
    instance_inner_types = _java_generic_inner_types(instance_java_type)
    if len(type_parameter_names) != len(instance_inner_types):
        return {}
    return dict(zip(type_parameter_names, instance_inner_types, strict=True))


def _infer_method_type_parameter_substitutions(
    method_declaration: javalang.tree.MethodDeclaration,
    argument_types: list[str | None],
) -> dict[str, str]:
    type_parameter_names = {
        type_parameter.name
        for type_parameter in method_declaration.type_parameters or []
    }
    substitutions: dict[str, str] = {}
    for parameter, argument_type in zip(
        method_declaration.parameters,
        argument_types,
        strict=True,
    ):
        if argument_type is None:
            continue
        _collect_method_type_parameter_substitutions(
            parameter_type=_render_type(parameter.type),
            argument_type=argument_type,
            type_parameter_names=type_parameter_names,
            substitutions=substitutions,
        )
    return substitutions


def _collect_method_type_parameter_substitutions(
    *,
    parameter_type: str,
    argument_type: str,
    type_parameter_names: set[str],
    substitutions: dict[str, str],
) -> None:
    if parameter_type in type_parameter_names:
        substitutions.setdefault(parameter_type, argument_type)
        return
    if parameter_type.endswith("[]") and argument_type.endswith("[]"):
        _collect_method_type_parameter_substitutions(
            parameter_type=parameter_type[:-2],
            argument_type=argument_type[:-2],
            type_parameter_names=type_parameter_names,
            substitutions=substitutions,
        )
        return
    parameter_base_type = _java_generic_base_type(parameter_type)
    argument_base_type = _java_generic_base_type(argument_type)
    if parameter_base_type != argument_base_type:
        return
    parameter_inner_types = _java_generic_inner_types(parameter_type)
    argument_inner_types = _java_generic_inner_types(argument_type)
    if len(parameter_inner_types) != len(argument_inner_types):
        return
    for nested_parameter_type, nested_argument_type in zip(
        parameter_inner_types,
        argument_inner_types,
        strict=True,
    ):
        _collect_method_type_parameter_substitutions(
            parameter_type=nested_parameter_type,
            argument_type=nested_argument_type,
            type_parameter_names=type_parameter_names,
            substitutions=substitutions,
        )


def _substitute_java_type_parameters(
    java_type: str,
    substitutions: dict[str, str],
) -> str:
    if java_type in substitutions:
        return substitutions[java_type]
    if java_type.endswith("[]"):
        return _substitute_java_type_parameters(java_type[:-2], substitutions) + "[]"
    base_type = _java_generic_base_type(java_type)
    inner_types = _java_generic_inner_types(java_type)
    if not inner_types:
        return substitutions.get(java_type, java_type)
    substituted_inner_types = ", ".join(
        _substitute_java_type_parameters(inner_type, substitutions)
        for inner_type in inner_types
    )
    return f"{base_type}<{substituted_inner_types}>"


def _infer_accessor_field_type(
    *,
    repo_root: Path,
    type_declaration: javalang.tree.TypeDeclaration,
    method_name: str,
    method_arity: int,
    import_map: dict[str, str],
    package_name: str | None,
) -> str | None:
    if method_arity != 0:
        return None
    candidate_field_names: list[str] = []
    if method_name.startswith("get") and len(method_name) > 3:
        candidate_field_names.append(_decapitalize_java_identifier(method_name[3:]))
    if method_name.startswith("is") and len(method_name) > 2:
        candidate_field_names.append(_decapitalize_java_identifier(method_name[2:]))
    for candidate_field_name in candidate_field_names:
        field_type = _find_field_type_declaration_in_hierarchy(
            repo_root=repo_root,
            type_declaration=type_declaration,
            import_map=import_map,
            package_name=package_name,
            field_name=candidate_field_name,
        )
        if field_type is not None:
            return field_type
    return None


def _decapitalize_java_identifier(value: str) -> str:
    if not value:
        return value
    if len(value) > 1 and value[0].isupper() and value[1].isupper():
        return value
    return value[0].lower() + value[1:]


def _find_field_type_declaration(
    type_declaration: javalang.tree.TypeDeclaration,
    field_name: str,
) -> str | None:
    for field in getattr(type_declaration, "fields", []):
        for declarator in field.declarators:
            if declarator.name == field_name:
                return _render_type(field.type)
    return None


def _find_field_type_declaration_in_hierarchy(
    *,
    repo_root: Path,
    type_declaration: javalang.tree.TypeDeclaration,
    import_map: dict[str, str],
    package_name: str | None,
    field_name: str,
    active_import_paths: tuple[str, ...] = (),
) -> str | None:
    direct_field_type = _find_field_type_declaration(type_declaration, field_name)
    if direct_field_type is not None:
        return direct_field_type
    if not isinstance(type_declaration, javalang.tree.ClassDeclaration):
        return None
    if type_declaration.extends is None:
        return None
    parent_import_path = resolve_referenced_import_path(
        repo_root,
        _render_reference_name(type_declaration.extends),
        import_map,
        package_name,
    )
    if parent_import_path is None or parent_import_path in active_import_paths:
        return None
    loaded_parent_type = _load_cached_type_declaration(repo_root, parent_import_path)
    if loaded_parent_type is None:
        return None
    _, parent_type_declaration, parent_import_map, parent_package_name = (
        loaded_parent_type
    )
    return _find_field_type_declaration_in_hierarchy(
        repo_root=repo_root,
        type_declaration=parent_type_declaration,
        import_map=parent_import_map,
        package_name=parent_package_name,
        field_name=field_name,
        active_import_paths=(*active_import_paths, parent_import_path),
    )
