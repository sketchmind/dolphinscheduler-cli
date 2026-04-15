"""DTO/model/enum extraction helpers."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Literal, cast

import javalang

from ds_codegen.extract.metadata import (
    _decode_scalar,
    _find_annotation_values,
    _get_bool_value,
    _get_string_value,
    _normalize_annotation_doc_text,
    _parse_doc_comment,
    _value_to_string,
)
from ds_codegen.extract.type_lookup import _render_reference_name, _render_type
from ds_codegen.ir import (
    DtoFieldSpec,
    DtoSpec,
    EnumFieldSpec,
    EnumSpec,
    EnumValueSpec,
    ModelKind,
    ModelSpec,
)
from ds_codegen.java_source import (
    BUILTIN_REFERENCE_TYPES,
)
from ds_codegen.java_source import (
    find_nested_type_declaration as _find_nested_type_declaration,
)
from ds_codegen.java_source import (
    load_type_declaration as _load_type_declaration,
)
from ds_codegen.java_source import (
    logical_type_name as _logical_type_name,
)
from ds_codegen.java_source import (
    parse_java_compilation_unit as _parse_java_compilation_unit,
)
from ds_codegen.java_source import (
    resolve_global_import_path as _resolve_global_import_path,
)
from ds_codegen.java_source import (
    resolve_import_path_with_nested as _resolve_import_path_with_nested,
)
from ds_codegen.java_source import (
    resolve_referenced_import_path as _resolve_referenced_import_path,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

REQUEST_DTO_SUFFIXES = (
    "CreateRequest",
    "UpdateRequest",
    "FilterRequest",
    "QueryRequest",
    "VerifyRequest",
    "Request",
)
_BASE_DATASOURCE_PARAM_DTO_IMPORT = (
    "org.apache.dolphinscheduler.plugin.datasource.api.datasource."
    "BaseDataSourceParamDTO"
)
DeclarationKind = Literal["class", "enum"]
_NON_NULL_ANNOTATIONS = {"NonNull", "Nonnull", "NotNull"}
_NULLABLE_ANNOTATIONS = {"CheckForNull", "Nullable"}
_EMPTY_COLLECTION_FACTORIES = {
    ("Collections", "emptyList"): "list",
    ("Collections", "emptySet"): "list",
    ("Collections", "emptyMap"): "dict",
}
_PRIMITIVE_DEFAULT_VALUES = {
    "boolean": "false",
    "byte": "0",
    "double": "0",
    "float": "0",
    "int": "0",
    "long": "0",
    "short": "0",
}
_JSON_DATA_SERIALIZER_NAMES = {
    "JSONUtils.JsonDataSerializer",
    "JsonDataSerializer",
}
_JSON_DATA_DESERIALIZER_NAMES = {
    "JSONUtils.JsonDataDeserializer",
    "JsonDataDeserializer",
}


def extract_dto_specs(
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
    dto_specs_by_import: dict[str, DtoSpec] = {}
    pending_imports = sorted(import_paths)
    enum_imports: set[str] = set()
    model_imports: set[str] = set()
    cache: dict[str, tuple[DtoSpec | None, set[str], set[str], set[str]]] = {}
    active: set[str] = set()

    while pending_imports:
        import_path = pending_imports.pop()
        (
            dto_spec,
            nested_dto_imports,
            nested_model_imports,
            nested_enum_imports,
        ) = _extract_dto_spec(
            repo_root,
            import_path,
            cache,
            active,
            parse_cache,
            declaration_kind_cache,
        )
        if dto_spec is None:
            continue
        dto_specs_by_import[import_path] = dto_spec
        for nested_dto_import in sorted(nested_dto_imports):
            if nested_dto_import not in dto_specs_by_import:
                pending_imports.append(nested_dto_import)
        model_imports.update(nested_model_imports)
        enum_imports.update(nested_enum_imports)

    return list(dto_specs_by_import.values()), model_imports, enum_imports


def extract_model_specs(
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
    model_specs_by_import: dict[str, ModelSpec] = {}
    pending_imports = sorted(import_paths)
    enum_imports: set[str] = set()
    discovered_model_imports: set[str] = set(import_paths)
    cache: dict[str, tuple[ModelSpec | None, set[str], set[str]]] = {}
    active: set[str] = set()

    while pending_imports:
        import_path = pending_imports.pop()
        model_spec, nested_model_imports, nested_enum_imports = _extract_model_spec(
            repo_root,
            import_path,
            cache,
            active,
            parse_cache,
            declaration_kind_cache,
        )
        if model_spec is None:
            continue
        model_specs_by_import[import_path] = model_spec
        enum_imports.update(nested_enum_imports)
        for nested_model_import in sorted(nested_model_imports):
            if nested_model_import in model_specs_by_import:
                continue
            if nested_model_import not in discovered_model_imports:
                discovered_model_imports.add(nested_model_import)
                pending_imports.append(nested_model_import)

    return list(model_specs_by_import.values()), enum_imports, discovered_model_imports


def extract_enum_specs(
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
    enum_specs: list[EnumSpec] = []
    for import_path in sorted(import_paths):
        loaded_declaration = _load_type_declaration(repo_root, import_path, parse_cache)
        if loaded_declaration is None:
            continue
        _, type_declaration, _, _ = loaded_declaration
        if not isinstance(type_declaration, javalang.tree.EnumDeclaration):
            continue
        enum_fields = _extract_enum_fields(type_declaration)
        enum_specs.append(
            EnumSpec(
                name=_logical_type_name(import_path),
                import_path=import_path,
                documentation=_parse_doc_comment(
                    type_declaration.documentation
                ).description,
                fields=enum_fields,
                json_value_field=_extract_enum_json_value_field(
                    type_declaration,
                    {field.name: field for field in enum_fields},
                ),
                values=[
                    EnumValueSpec(
                        name=enum_constant.name,
                        arguments=[
                            _value_to_string(_decode_scalar(argument))
                            for argument in enum_constant.arguments or []
                        ],
                        documentation=_parse_doc_comment(
                            enum_constant.documentation
                        ).description,
                    )
                    for enum_constant in type_declaration.body.constants
                ],
            )
        )
    return enum_specs


def collect_generated_view_model_imports(
    repo_root: Path,
    generated_view_models: list[ModelSpec],
    known_type_names: set[str],
) -> set[str]:
    additional_imports: set[str] = set()
    for generated_view_model in generated_view_models:
        for field in generated_view_model.fields:
            for referenced_type in collect_type_reference_names_from_java_type(
                field.java_type
            ):
                if referenced_type in known_type_names:
                    continue
                import_path = _resolve_global_import_path(
                    repo_root,
                    referenced_type,
                )
                if import_path is not None:
                    additional_imports.add(import_path)
    return additional_imports


def collect_type_reference_names(type_node: object | None) -> set[str]:
    if type_node is None:
        return set()
    if isinstance(type_node, javalang.tree.BasicType):
        return set()
    if not isinstance(type_node, javalang.tree.ReferenceType):
        return set()
    names = {_render_reference_name(type_node)}
    if type_node.arguments:
        for argument in type_node.arguments:
            if isinstance(argument, javalang.tree.TypeArgument):
                names.update(collect_type_reference_names(argument.type))
            else:
                names.update(collect_type_reference_names(argument))
    return names


def collect_type_reference_names_from_java_type(java_type: str) -> set[str]:
    if java_type in BUILTIN_REFERENCE_TYPES | {"Any"}:
        return set()
    if java_type.endswith("[]"):
        return collect_type_reference_names_from_java_type(java_type[:-2])
    generic_start = java_type.find("<")
    if generic_start != -1 and java_type.endswith(">"):
        base_type = java_type[:generic_start]
        names = collect_type_reference_names_from_java_type(base_type)
        for inner_type in _split_top_level_generic_types(
            java_type[generic_start + 1 : -1]
        ):
            names.update(collect_type_reference_names_from_java_type(inner_type))
        return names
    return {java_type}


def looks_like_request_dto_import(import_path: str) -> bool:
    logical_name = _logical_type_name(import_path).split(".")[-1]
    return logical_name.endswith(REQUEST_DTO_SUFFIXES)


def _extract_dto_spec(
    repo_root: Path,
    import_path: str,
    cache: dict[str, tuple[DtoSpec | None, set[str], set[str], set[str]]],
    active: set[str],
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
) -> tuple[DtoSpec | None, set[str], set[str], set[str]]:
    cached = cache.get(import_path)
    if cached is not None:
        return cached
    if import_path in active:
        return None, set(), set(), set()
    active.add(import_path)

    loaded_declaration = _load_type_declaration(repo_root, import_path, parse_cache)
    if loaded_declaration is None:
        active.remove(import_path)
        cache[import_path] = (None, set(), set(), set())
        return cache[import_path]
    _, type_declaration, import_map, package_name = loaded_declaration
    if not isinstance(type_declaration, javalang.tree.ClassDeclaration):
        active.remove(import_path)
        cache[import_path] = (None, set(), set(), set())
        return cache[import_path]

    fields: list[DtoFieldSpec] = []
    dto_imports: set[str] = set()
    model_imports: set[str] = set()
    enum_imports: set[str] = set()
    nested_type_names = _collect_nested_type_names(type_declaration)
    extends_name = (
        _render_reference_name(type_declaration.extends)
        if type_declaration.extends is not None
        else None
    )
    if extends_name is not None:
        parent_import_path = _resolve_referenced_import_path(
            repo_root,
            extends_name,
            import_map,
            package_name,
        )
        if parent_import_path is not None:
            (
                parent_spec,
                parent_dto_imports,
                parent_model_imports,
                parent_enum_imports,
            ) = _extract_dto_spec(
                repo_root,
                parent_import_path,
                cache,
                active,
                parse_cache,
                declaration_kind_cache,
            )
            if parent_spec is not None:
                fields.extend(parent_spec.fields)
            dto_imports.update(parent_dto_imports)
            model_imports.update(parent_model_imports)
            enum_imports.update(parent_enum_imports)
            dto_imports.add(parent_import_path)

    for field in type_declaration.fields:
        if not _is_instance_field(field):
            continue
        fields.append(
            _qualify_field_spec_nested_types(
                _extract_dto_field_spec(field),
                import_path,
                nested_type_names,
            )
        )
        field_dto_imports, field_model_imports, field_enum_imports = (
            _collect_field_dependency_imports(
                field.type,
                import_map,
                package_name,
                repo_root,
                declaration_kind_cache,
                import_path,
                nested_type_names,
            )
        )
        dto_imports.update(field_dto_imports)
        model_imports.update(field_model_imports)
        enum_imports.update(field_enum_imports)

    active.remove(import_path)
    cache[import_path] = (
        DtoSpec(
            name=_logical_type_name(import_path),
            import_path=import_path,
            documentation=_parse_doc_comment(
                type_declaration.documentation
            ).description,
            extends=extends_name,
            fields=fields,
        ),
        dto_imports,
        model_imports,
        enum_imports,
    )
    return cache[import_path]


def _extract_model_spec(
    repo_root: Path,
    import_path: str,
    cache: dict[str, tuple[ModelSpec | None, set[str], set[str]]],
    active: set[str],
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
) -> tuple[ModelSpec | None, set[str], set[str]]:
    cached = cache.get(import_path)
    if cached is not None:
        return cached
    if import_path in active:
        return None, set(), set()
    active.add(import_path)

    loaded_declaration = _load_type_declaration(repo_root, import_path, parse_cache)
    if loaded_declaration is None:
        active.remove(import_path)
        cache[import_path] = (None, set(), set())
        return cache[import_path]
    _, type_declaration, import_map, package_name = loaded_declaration
    if isinstance(type_declaration, javalang.tree.EnumDeclaration):
        active.remove(import_path)
        cache[import_path] = (None, set(), set())
        return cache[import_path]
    if not isinstance(type_declaration, javalang.tree.ClassDeclaration):
        active.remove(import_path)
        cache[import_path] = (None, set(), set())
        return cache[import_path]

    fields: list[DtoFieldSpec] = []
    model_imports: set[str] = set()
    enum_imports: set[str] = set()
    nested_type_names = _collect_nested_type_names(type_declaration)
    extends_name = (
        _render_reference_name(type_declaration.extends)
        if type_declaration.extends is not None
        else None
    )
    if extends_name is not None:
        parent_import_path = _resolve_referenced_import_path(
            repo_root,
            extends_name,
            import_map,
            package_name,
        )
        if parent_import_path is not None:
            parent_spec, parent_model_imports, parent_enum_imports = (
                _extract_model_spec(
                    repo_root,
                    parent_import_path,
                    cache,
                    active,
                    parse_cache,
                    declaration_kind_cache,
                )
            )
            if parent_spec is not None:
                fields.extend(parent_spec.fields)
            model_imports.update(parent_model_imports)
            enum_imports.update(parent_enum_imports)
            model_imports.add(parent_import_path)

    for field in type_declaration.fields:
        if not _is_instance_field(field):
            continue
        fields.append(
            _qualify_field_spec_nested_types(
                _extract_field_spec(field),
                import_path,
                nested_type_names,
            )
        )
        field_dto_imports, field_model_imports, field_enum_imports = (
            _collect_field_dependency_imports(
                field.type,
                import_map,
                package_name,
                repo_root,
                declaration_kind_cache,
                import_path,
                nested_type_names,
            )
        )
        model_imports.update(field_dto_imports)
        model_imports.update(field_model_imports)
        enum_imports.update(field_enum_imports)

    synthetic_fields, synthetic_enum_imports = _synthetic_model_fields(
        repo_root=repo_root,
        import_path=import_path,
        import_map=import_map,
        package_name=package_name,
        existing_fields=fields,
    )
    fields.extend(synthetic_fields)
    enum_imports.update(synthetic_enum_imports)
    fields = _normalize_model_fields(import_path, fields)

    model_spec = ModelSpec(
        name=_logical_type_name(import_path),
        import_path=import_path,
        kind=_classify_model_kind(import_path),
        documentation=_parse_doc_comment(type_declaration.documentation).description,
        extends=extends_name,
        fields=fields,
    )
    active.remove(import_path)
    cache[import_path] = (model_spec, model_imports, enum_imports)
    return cache[import_path]


def _synthetic_model_fields(
    *,
    repo_root: Path,
    import_path: str,
    import_map: dict[str, str],
    package_name: str | None,
    existing_fields: list[DtoFieldSpec],
) -> tuple[list[DtoFieldSpec], set[str]]:
    if import_path != _BASE_DATASOURCE_PARAM_DTO_IMPORT:
        return [], set()
    if any(field.wire_name == "type" for field in existing_fields):
        return [], set()
    db_type_import_path = _resolve_referenced_import_path(
        repo_root,
        "DbType",
        import_map,
        package_name,
    )
    synthetic_field = DtoFieldSpec(
        name="type",
        java_type="DbType",
        wire_name="type",
        required=False,
        default_value=None,
        nullable=True,
        default_factory=None,
        description="datasource type",
        example=None,
        allowable_values=None,
        documentation="Datasource type.",
    )
    if db_type_import_path is None:
        return [synthetic_field], set()
    return [synthetic_field], {db_type_import_path}


def _normalize_model_fields(
    import_path: str,
    fields: list[DtoFieldSpec],
) -> list[DtoFieldSpec]:
    if import_path != "org.apache.dolphinscheduler.api.utils.PageInfo":
        return fields
    return [
        (
            DtoFieldSpec(
                name=field.name,
                java_type=field.java_type,
                wire_name=field.wire_name,
                required=field.required,
                default_value=field.default_value,
                nullable=True,
                default_factory=field.default_factory,
                description=field.description,
                example=field.example,
                allowable_values=field.allowable_values,
                documentation=field.documentation,
            )
            if field.wire_name == "currentPage"
            else field
        )
        for field in fields
    ]


def _extract_dto_field_spec(field: javalang.tree.FieldDeclaration) -> DtoFieldSpec:
    return _extract_field_spec(field)


def _extract_field_spec(field: javalang.tree.FieldDeclaration) -> DtoFieldSpec:
    declarator = field.declarators[0]
    schema_values = _find_annotation_values(field.annotations, "Schema")
    explicit_wire_name = _get_string_value(schema_values, "name")
    wire_name = explicit_wire_name or _default_field_wire_name(field, declarator.name)
    default_value = _get_string_value(schema_values, "defaultValue")
    if default_value is None and declarator.initializer is not None:
        default_value = _scalar_initializer_default_value(declarator.initializer)
    if default_value is None and isinstance(field.type, javalang.tree.BasicType):
        default_value = _PRIMITIVE_DEFAULT_VALUES.get(field.type.name)
    return DtoFieldSpec(
        name=declarator.name,
        java_type=_field_wire_java_type(field),
        wire_name=wire_name,
        required=_get_bool_value(schema_values, "required"),
        default_value=default_value,
        nullable=_field_is_nullable(field),
        default_factory=_field_default_factory(field),
        description=_normalize_annotation_doc_text(
            _get_string_value(schema_values, "description")
        ),
        example=_normalize_annotation_doc_text(
            _get_string_value(schema_values, "example")
        ),
        allowable_values=_normalize_annotation_doc_text(
            _get_string_value(schema_values, "allowableValues")
        ),
        documentation=_parse_doc_comment(field.documentation).description,
    )


def _field_wire_java_type(field: javalang.tree.FieldDeclaration) -> str:
    java_type = _render_type(field.type)
    if java_type != "String":
        return java_type
    if _field_uses_json_data_codec(field):
        return "JsonValue"
    return java_type


def _field_uses_json_data_codec(field: javalang.tree.FieldDeclaration) -> bool:
    serialize_values = _find_annotation_values(field.annotations, "JsonSerialize")
    deserialize_values = _find_annotation_values(field.annotations, "JsonDeserialize")
    return _annotation_uses_any_codec(
        serialize_values,
        expected_names=_JSON_DATA_SERIALIZER_NAMES,
    ) or _annotation_uses_any_codec(
        deserialize_values,
        expected_names=_JSON_DATA_DESERIALIZER_NAMES,
    )


def _annotation_uses_any_codec(
    values: Mapping[str, object],
    *,
    expected_names: set[str],
) -> bool:
    for key in ("using", "value"):
        codec_value = values.get(key)
        codec_name = codec_value if isinstance(codec_value, str) else None
        if codec_name is None:
            continue
        if any(codec_name.endswith(expected_name) for expected_name in expected_names):
            return True
    return False


def _default_field_wire_name(
    field: javalang.tree.FieldDeclaration,
    field_name: str,
) -> str:
    if _uses_boolean_bean_wire_name(field, field_name):
        return _java_bean_decapitalize(field_name[2:])
    return field_name


def _uses_boolean_bean_wire_name(
    field: javalang.tree.FieldDeclaration,
    field_name: str,
) -> bool:
    if not (
        field_name.startswith("is") and len(field_name) > 2 and field_name[2].isupper()
    ):
        return False
    return _render_type(field.type) in {"Boolean", "boolean"}


def _java_bean_decapitalize(value: str) -> str:
    if len(value) >= 2 and value[0].isupper() and value[1].isupper():
        return value
    return value[:1].lower() + value[1:]


def _classify_model_kind(import_path: str) -> ModelKind:
    if ".api.dto." in import_path:
        return "api_dto"
    if ".api.utils." in import_path:
        return "api_util"
    if ".dao.entity." in import_path:
        return "dao_entity"
    return "other_class"


def _extract_enum_fields(
    type_declaration: javalang.tree.EnumDeclaration,
) -> list[EnumFieldSpec]:
    declared_fields: dict[str, EnumFieldSpec] = {}
    for declaration in type_declaration.body.declarations:
        if not isinstance(declaration, javalang.tree.FieldDeclaration):
            continue
        if not _is_instance_field(declaration):
            continue
        java_type = _render_type(declaration.type)
        annotations = [annotation.name for annotation in declaration.annotations]
        for declarator in declaration.declarators:
            declared_fields[declarator.name] = EnumFieldSpec(
                name=declarator.name,
                java_type=java_type,
                annotations=annotations,
            )

    constructor_fields: list[EnumFieldSpec] = []
    for declaration in type_declaration.body.declarations:
        if not isinstance(declaration, javalang.tree.ConstructorDeclaration):
            continue
        parameter_to_field = _extract_enum_constructor_field_mapping(declaration)
        for parameter in declaration.parameters:
            field_name = parameter_to_field.get(parameter.name, parameter.name)
            declared_field = declared_fields.get(field_name)
            constructor_fields.append(
                EnumFieldSpec(
                    name=field_name,
                    java_type=(
                        declared_field.java_type
                        if declared_field is not None
                        else _render_type(parameter.type)
                    ),
                    annotations=(
                        declared_field.annotations if declared_field is not None else []
                    ),
                )
            )
        break

    if constructor_fields:
        return constructor_fields
    return list(declared_fields.values())


def _extract_enum_constructor_field_mapping(
    declaration: javalang.tree.ConstructorDeclaration,
) -> dict[str, str]:
    parameter_to_field: dict[str, str] = {}
    for statement in declaration.body or []:
        if not isinstance(statement, javalang.tree.StatementExpression):
            continue
        expression = statement.expression
        if not isinstance(expression, javalang.tree.Assignment):
            continue
        parameter_name = _extract_member_reference_name(expression.value)
        field_name = _extract_assigned_field_name(expression.expressionl)
        if parameter_name is None or field_name is None:
            continue
        parameter_to_field[parameter_name] = field_name
    return parameter_to_field


def _extract_assigned_field_name(expression: object) -> str | None:
    if isinstance(expression, javalang.tree.MemberReference):
        return cast("str", expression.member)
    if isinstance(expression, javalang.tree.This) and expression.selectors:
        selector = expression.selectors[0]
        if isinstance(selector, javalang.tree.MemberReference):
            return cast("str", selector.member)
    return None


def _extract_member_reference_name(expression: object) -> str | None:
    if isinstance(expression, javalang.tree.MemberReference):
        return cast("str", expression.member)
    return None


def _extract_enum_json_value_field(
    type_declaration: javalang.tree.EnumDeclaration,
    fields_by_name: dict[str, EnumFieldSpec],
) -> str | None:
    for declaration in type_declaration.body.declarations:
        if not isinstance(declaration, javalang.tree.FieldDeclaration):
            continue
        if not any(
            annotation.name == "JsonValue" for annotation in declaration.annotations
        ):
            continue
        if len(declaration.declarators) == 1:
            return cast("str", declaration.declarators[0].name)

    for declaration in type_declaration.body.declarations:
        if not isinstance(declaration, javalang.tree.MethodDeclaration):
            continue
        if not any(
            annotation.name == "JsonValue" for annotation in declaration.annotations
        ):
            continue
        if not declaration.body or len(declaration.body) != 1:
            continue
        statement = declaration.body[0]
        if not isinstance(statement, javalang.tree.ReturnStatement):
            continue
        returned_field = _extract_json_value_return_field(statement.expression)
        if returned_field is not None and returned_field in fields_by_name:
            return returned_field
    return None


def _extract_json_value_return_field(expression: object) -> str | None:
    if isinstance(expression, javalang.tree.MemberReference):
        return cast("str", expression.member)
    if isinstance(expression, javalang.tree.This) and expression.selectors:
        selector = expression.selectors[0]
        if isinstance(selector, javalang.tree.MemberReference):
            return cast("str", selector.member)
    return None


def _collect_nested_type_names(
    type_declaration: javalang.tree.TypeDeclaration,
) -> set[str]:
    nested_names: set[str] = set()
    for body_declaration in getattr(type_declaration, "body", None) or []:
        if not isinstance(
            body_declaration,
            (
                javalang.tree.ClassDeclaration,
                javalang.tree.EnumDeclaration,
                javalang.tree.InterfaceDeclaration,
            ),
        ):
            continue
        nested_names.add(body_declaration.name)
    return nested_names


def _qualify_field_spec_nested_types(
    field_spec: DtoFieldSpec,
    owner_import_path: str,
    nested_type_names: set[str],
) -> DtoFieldSpec:
    if not nested_type_names:
        return field_spec
    owner_logical_name = _logical_type_name(owner_import_path)
    qualified_java_type = field_spec.java_type
    for nested_type_name in sorted(nested_type_names):
        qualified_java_type = re.sub(
            rf"\b{re.escape(nested_type_name)}\b",
            f"{owner_logical_name}.{nested_type_name}",
            qualified_java_type,
        )
    if qualified_java_type == field_spec.java_type:
        return field_spec
    return DtoFieldSpec(
        name=field_spec.name,
        java_type=qualified_java_type,
        wire_name=field_spec.wire_name,
        required=field_spec.required,
        default_value=field_spec.default_value,
        nullable=field_spec.nullable,
        default_factory=field_spec.default_factory,
        description=field_spec.description,
        example=field_spec.example,
        allowable_values=field_spec.allowable_values,
        documentation=field_spec.documentation,
    )


def _field_is_nullable(field: javalang.tree.FieldDeclaration) -> bool:
    if isinstance(field.type, javalang.tree.BasicType):
        return False
    annotation_names = {
        annotation.name.split(".")[-1] for annotation in field.annotations
    }
    if annotation_names & _NULLABLE_ANNOTATIONS:
        return True
    if annotation_names & _NON_NULL_ANNOTATIONS:
        return False
    initializer = field.declarators[0].initializer
    return not _field_initializer_is_definitely_non_null(initializer)


def _is_instance_field(field: javalang.tree.FieldDeclaration) -> bool:
    return "static" not in field.modifiers


def _field_initializer_is_definitely_non_null(initializer: object | None) -> bool:
    if initializer is None:
        return False
    if isinstance(initializer, javalang.tree.Literal):
        return isinstance(initializer.value, str) and initializer.value != "null"
    if isinstance(
        initializer,
        (
            javalang.tree.ArrayCreator,
            javalang.tree.ClassCreator,
        ),
    ):
        return True
    if isinstance(initializer, javalang.tree.MethodInvocation):
        qualifier = initializer.qualifier or ""
        factory = _EMPTY_COLLECTION_FACTORIES.get((qualifier, initializer.member))
        return factory is not None
    return False


def _field_default_factory(field: javalang.tree.FieldDeclaration) -> str | None:
    initializer = field.declarators[0].initializer
    if isinstance(initializer, javalang.tree.ArrayCreator):
        return "list"
    if isinstance(initializer, javalang.tree.ClassCreator):
        created_type = _render_type(initializer.type)
        if _looks_like_mapping_type(created_type):
            return "dict"
        if _looks_like_sequence_type(created_type):
            return "list"
        return None
    if isinstance(initializer, javalang.tree.MethodInvocation):
        qualifier = initializer.qualifier or ""
        return _EMPTY_COLLECTION_FACTORIES.get((qualifier, initializer.member))
    return None


def _looks_like_mapping_type(java_type: str) -> bool:
    return java_type.startswith(
        (
            "HashMap",
            "LinkedHashMap",
            "Map",
            "ObjectNode",
            "TreeMap",
        )
    )


def _looks_like_sequence_type(java_type: str) -> bool:
    return java_type.startswith(
        (
            "ArrayList",
            "Collection",
            "LinkedList",
            "List",
            "Set",
        )
    )


def _scalar_initializer_default_value(initializer: object) -> str | None:
    if isinstance(initializer, javalang.tree.Literal):
        decoded_initializer = _decode_scalar(initializer)
        return _value_to_string(decoded_initializer)
    if isinstance(initializer, javalang.tree.MemberReference):
        decoded_initializer = _decode_scalar(initializer)
        return decoded_initializer if isinstance(decoded_initializer, str) else None
    return None


def _collect_field_dependency_imports(
    type_node: object | None,
    import_map: dict[str, str],
    package_name: str | None,
    repo_root: Path,
    declaration_kind_cache: dict[str, DeclarationKind | None],
    owner_import_path: str | None = None,
    nested_type_names: set[str] | None = None,
) -> tuple[set[str], set[str], set[str]]:
    dto_imports: set[str] = set()
    model_imports: set[str] = set()
    enum_imports: set[str] = set()
    for referenced_type in collect_type_reference_names(type_node):
        import_path = _resolve_referenced_import_path(
            repo_root,
            referenced_type,
            import_map,
            package_name,
            owner_import_path,
            nested_type_names or set(),
        )
        if import_path is None:
            continue
        declaration_kind = _declaration_kind_from_import_path(
            repo_root,
            import_path,
            declaration_kind_cache,
        )
        if declaration_kind == "enum":
            enum_imports.add(import_path)
            continue
        if declaration_kind != "class":
            continue
        if looks_like_request_dto_import(import_path):
            dto_imports.add(import_path)
        else:
            model_imports.add(import_path)
    return dto_imports, model_imports, enum_imports


def _split_top_level_generic_types(value: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    for index, char in enumerate(value):
        if char == "<":
            depth += 1
        elif char == ">":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(value[start:index].strip())
            start = index + 1
    tail = value[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def _declaration_kind_from_import_path(
    repo_root: Path,
    import_path: str,
    declaration_kind_cache: dict[str, DeclarationKind | None],
) -> DeclarationKind | None:
    if import_path in declaration_kind_cache:
        return declaration_kind_cache[import_path]

    resolved = _resolve_import_path_with_nested(repo_root, import_path)
    if resolved is None:
        declaration_kind_cache[import_path] = None
        return None
    declaration_file, nested_names = resolved
    source = declaration_file.read_text()
    compilation_unit = _parse_java_compilation_unit(source)
    type_declaration = compilation_unit.types[0]
    nested_declaration = _find_nested_type_declaration(type_declaration, nested_names)
    if isinstance(nested_declaration, javalang.tree.EnumDeclaration):
        declaration_kind_cache[import_path] = "enum"
        return "enum"
    if isinstance(nested_declaration, javalang.tree.ClassDeclaration):
        declaration_kind_cache[import_path] = "class"
        return "class"
    declaration_kind_cache[import_path] = None
    return None
