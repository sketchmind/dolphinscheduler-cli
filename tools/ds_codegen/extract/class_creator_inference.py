"""Class creator and constructor-based return inference."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import javalang

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


@dataclass(frozen=True)
class ClassCreatorInferenceDeps:
    render_reference_name: Callable[[javalang.tree.ReferenceType], str]
    infer_expression_data_type: Callable[..., str | None]
    infer_expression_return_type: Callable[..., str | None]
    infer_argument_types: Callable[..., list[str | None]]
    resolve_referenced_import_path: Callable[..., str | None]
    type_extends_result: Callable[[Path, str], bool]
    load_java_type_context: Callable[
        ...,
        tuple[object, object, dict[str, str], str | None] | None,
    ]
    find_constructor_declaration: Callable[
        ...,
        javalang.tree.ConstructorDeclaration | None,
    ]
    find_field_type_declaration_in_hierarchy: Callable[..., str | None]
    register_generated_view_model: Callable[..., str]


def infer_class_creator_return_type(
    *,
    repo_root: Path,
    controller_path: Path,
    class_creator: javalang.tree.ClassCreator,
    controller_field_types: dict[str, str],
    variable_types: dict[str, str],
    variable_initializers: dict[str, object],
    import_map: dict[str, str],
    package_name: str | None,
    deps: ClassCreatorInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    created_type_name = deps.render_reference_name(class_creator.type)
    structured_type = infer_class_creator_structured_type(
        repo_root=repo_root,
        controller_path=controller_path,
        class_creator=class_creator,
        view_name_hint=f"{controller_path.stem}_{created_type_name}_created",
        controller_field_types=controller_field_types,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        import_map=import_map,
        package_name=package_name,
        deps=deps,
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )
    if structured_type is not None:
        return structured_type
    if created_type_name == "Result":
        if len(class_creator.arguments) in {1, 2}:
            return "Void"
        if len(class_creator.arguments) == 3:
            return deps.infer_expression_data_type(
                repo_root=repo_root,
                controller_path=controller_path,
                expression=class_creator.arguments[2],
                controller_field_types=controller_field_types,
                variable_types=variable_types,
                variable_initializers=variable_initializers,
                import_map=import_map,
                package_name=package_name,
                owner_methods=owner_methods,
                active_same_class_methods=active_same_class_methods,
            )
        return None
    import_path = deps.resolve_referenced_import_path(
        repo_root,
        created_type_name,
        import_map,
        package_name,
    )
    if import_path is None or not deps.type_extends_result(repo_root, import_path):
        return None
    if not class_creator.arguments:
        return "Void"
    if len(class_creator.arguments) != 1:
        return None
    return deps.infer_expression_return_type(
        repo_root=repo_root,
        controller_path=controller_path,
        expression=class_creator.arguments[0],
        controller_field_types=controller_field_types,
        variable_types=variable_types,
        variable_initializers=variable_initializers,
        import_map=import_map,
        package_name=package_name,
        owner_methods=owner_methods,
        active_same_class_methods=active_same_class_methods,
    )


def infer_class_creator_structured_type(
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
    deps: ClassCreatorInferenceDeps,
    owner_methods: list[javalang.tree.MethodDeclaration] | None = None,
    active_same_class_methods: tuple[tuple[str, int], ...] = (),
) -> str | None:
    created_type_name = deps.render_reference_name(class_creator.type)
    if created_type_name in {
        "ArrayList",
        "LinkedList",
        "HashMap",
        "LinkedHashMap",
        "TreeMap",
        "Result",
    }:
        return None
    loaded_type = deps.load_java_type_context(
        repo_root=repo_root,
        java_type=created_type_name,
        import_map=import_map,
        package_name=package_name,
    )
    if loaded_type is None:
        return None
    _, type_declaration, type_import_map, type_package_name = loaded_type
    if not isinstance(type_declaration, javalang.tree.ClassDeclaration):
        return None
    constructor = deps.find_constructor_declaration(
        type_declaration.constructors,
        len(class_creator.arguments),
        argument_types=deps.infer_argument_types(
            repo_root=repo_root,
            controller_path=controller_path,
            arguments=class_creator.arguments,
            controller_field_types=controller_field_types,
            variable_types=variable_types,
            variable_initializers=variable_initializers,
            import_map=import_map,
            package_name=package_name,
            owner_methods=owner_methods,
            active_same_class_methods=active_same_class_methods,
        ),
    )
    if constructor is None or not constructor.parameters:
        return None
    fields: list[tuple[str, str]] = []
    for parameter, argument in zip(
        constructor.parameters, class_creator.arguments, strict=True
    ):
        field_type = deps.find_field_type_declaration_in_hierarchy(
            repo_root=repo_root,
            type_declaration=type_declaration,
            import_map=type_import_map,
            package_name=type_package_name,
            field_name=parameter.name,
        )
        if field_type is None:
            return None
        argument_java_type = deps.infer_expression_data_type(
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
        if argument_java_type is None:
            return None
        fields.append((parameter.name, argument_java_type))
    if not fields:
        return None
    return deps.register_generated_view_model(
        base_name=view_name_hint,
        fields=fields,
    )


def find_constructor_declaration(
    constructors: list[javalang.tree.ConstructorDeclaration],
    constructor_arity: int,
    argument_types: list[str | None] | None = None,
    score_parameter_argument_match: Callable[..., int | None] | None = None,
) -> javalang.tree.ConstructorDeclaration | None:
    exact_matches = [
        constructor
        for constructor in constructors
        if len(constructor.parameters) == constructor_arity
    ]
    if len(exact_matches) == 1:
        return exact_matches[0]
    if (
        exact_matches
        and argument_types is not None
        and score_parameter_argument_match is not None
    ):
        best_matches: list[javalang.tree.ConstructorDeclaration] = []
        best_score: int | None = None
        for constructor in exact_matches:
            score = score_parameter_argument_match(
                constructor.parameters,
                argument_types,
            )
            if score is None:
                continue
            if best_score is None or score > best_score:
                best_matches = [constructor]
                best_score = score
                continue
            if score == best_score:
                best_matches.append(constructor)
        if len(best_matches) == 1:
            return best_matches[0]
    return None
