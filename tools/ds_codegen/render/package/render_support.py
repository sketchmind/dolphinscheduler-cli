"""Shared formatting helpers for generated package renderers."""

from __future__ import annotations

import keyword
import re
import textwrap
from typing import TYPE_CHECKING

from ds_codegen.java_literals import parse_java_numeric_literal

if TYPE_CHECKING:
    from ds_codegen.ir import DtoFieldSpec, ParameterSpec

_PYDANTIC_RESERVED_FIELD_NAMES = {
    "construct",
    "copy",
    "dict",
    "json",
    "model_computed_fields",
    "model_config",
    "model_construct",
    "model_copy",
    "model_dump",
    "model_dump_json",
    "model_extra",
    "model_fields",
    "model_fields_set",
    "model_json_schema",
    "model_parametrized_name",
    "model_post_init",
    "model_rebuild",
    "model_validate",
    "model_validate_json",
    "model_validate_strings",
    "parse_file",
    "parse_obj",
    "parse_raw",
    "schema",
    "schema_json",
    "update_forward_refs",
    "validate",
}


def render_docstring_lines(docstring: str, *, indent: str) -> list[str]:
    doc_lines = docstring.split("\n")
    if len(doc_lines) == 1:
        return [f'{indent}"""{doc_lines[0]}"""']
    lines = [f'{indent}"""']
    lines.extend(f"{indent}{line}" if line else indent for line in doc_lines)
    lines.append(f'{indent}"""')
    return lines


def display_doc_text(text: str) -> str:
    if not text:
        return text
    return text[0].upper() + text[1:]


def wrap_comment_lines(text: str, *, indent: str) -> list[str]:
    wrapped_lines: list[str] = []
    for raw_paragraph in text.split("\n\n"):
        paragraph = raw_paragraph.strip()
        if not paragraph:
            continue
        wrapped_lines.extend(
            f"{indent}# {line}"
            for line in textwrap.wrap(
                paragraph,
                width=88 - len(indent) - 2,
                break_long_words=False,
                break_on_hyphens=False,
            )
        )
    return wrapped_lines


def render_example_literal(java_type: str, example: str) -> str:
    normalized_type = java_type
    while normalized_type.startswith("Optional<") and normalized_type.endswith(">"):
        normalized_type = normalized_type[9:-1]
    numeric_value = parse_java_numeric_literal(example)
    if normalized_type in {
        "Byte",
        "Integer",
        "Long",
        "Short",
        "byte",
        "int",
        "long",
        "short",
    }:
        if isinstance(numeric_value, int) and not isinstance(numeric_value, bool):
            return repr(numeric_value)
        return repr(example)
    if normalized_type in {"Double", "Float", "double", "float"}:
        if isinstance(numeric_value, (int, float)) and not isinstance(
            numeric_value,
            bool,
        ):
            return repr(float(numeric_value))
        return repr(example)
    if normalized_type in {"Boolean", "boolean"}:
        lowered = example.lower()
        if lowered == "true":
            return "True"
        if lowered == "false":
            return "False"
        return repr(example)
    return repr(example)


def render_allowable_values_literal(java_type: str, allowable_values: str) -> str:
    separators = (r"\s*/\s*", r"\s*,\s*")
    parts = [allowable_values]
    for separator in separators:
        candidate_parts = [
            part for part in re.split(separator, allowable_values) if part
        ]
        if len(candidate_parts) > 1:
            parts = candidate_parts
            break
    if len(parts) == 1:
        return repr(allowable_values)
    rendered_parts = [render_example_literal(java_type, part.strip()) for part in parts]
    return "[" + ", ".join(rendered_parts) + "]"


def render_pydantic_field_config(
    field: DtoFieldSpec,
    *,
    required: bool,
    attribute_name: str,
    default_expression: str | None = None,
    default_factory: str | None = None,
) -> str | None:
    args: list[str] = []
    if not required and default_factory is not None:
        args.append(f"default_factory={default_factory}")
    elif not required and default_expression is not None:
        args.append(f"default={default_expression}")
    elif not required:
        args.append("default=None")
    if attribute_name != field.wire_name:
        args.append(f"alias={field.wire_name!r}")
    if field.description is not None:
        args.append(f"description={field.description!r}")
    elif field.documentation is not None:
        args.append(f"description={field.documentation!r}")
    if field.example is not None:
        rendered_example = render_example_literal(field.java_type, field.example)
        args.append(f"examples=[{rendered_example}]")
    if field.allowable_values is not None:
        rendered_allowable_values = render_allowable_values_literal(
            field.java_type,
            field.allowable_values,
        )
        args.append(
            f"json_schema_extra={{'allowable_values': {rendered_allowable_values}}}"
        )
    return ", ".join(args) if args else None


def render_field_default_expression(field: DtoFieldSpec) -> str | None:
    if field.default_value is None:
        return None
    symbolic_default = _render_symbolic_default_expression(
        field.java_type,
        field.default_value,
    )
    if symbolic_default is not None:
        return symbolic_default
    return render_example_literal(field.java_type, field.default_value)


def render_parameter_field_config(
    parameter: ParameterSpec,
    *,
    required: bool,
    attribute_name: str,
) -> str | None:
    args: list[str] = []
    if not required:
        args.append("default=None")
    wire_name = parameter.wire_name or parameter.name
    if attribute_name != wire_name:
        args.append(f"alias={wire_name!r}")
    if parameter.description is not None:
        args.append(f"description={parameter.description!r}")
    if parameter.example is not None:
        rendered_example = render_example_literal(
            parameter.java_type,
            parameter.example,
        )
        args.append(f"examples=[{rendered_example}]")
    if parameter.allowable_values is not None:
        rendered_allowable_values = render_allowable_values_literal(
            parameter.java_type,
            parameter.allowable_values,
        )
        args.append(
            f"json_schema_extra={{'allowable_values': {rendered_allowable_values}}}"
        )
    return ", ".join(args) if args else None


def pydantic_field_name(wire_name: str) -> str:
    if keyword.iskeyword(wire_name) or wire_name in _PYDANTIC_RESERVED_FIELD_NAMES:
        return f"{wire_name}_field"
    return wire_name


def _render_symbolic_default_expression(
    java_type: str,
    default_value: str,
) -> str | None:
    normalized_type = java_type
    while normalized_type.startswith("Optional<") and normalized_type.endswith(">"):
        normalized_type = normalized_type[9:-1]
    if default_value == "null":
        return "None"
    if _looks_like_qualified_member_reference(default_value):
        qualifier, _, _ = default_value.rpartition(".")
        if qualifier.split(".")[-1] == _simple_type_name(normalized_type):
            return default_value
        return None
    if _looks_like_symbolic_member_name(default_value) and _looks_like_symbolic_type(
        normalized_type
    ):
        return f"{_simple_type_name(normalized_type)}.{default_value}"
    return None


def _looks_like_qualified_member_reference(value: str) -> bool:
    return bool(
        re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_.]*\.[A-Za-z_][A-Za-z0-9_]*",
            value,
        )
    )


def _looks_like_symbolic_member_name(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Z][A-Z0-9_]*", value))


def _looks_like_symbolic_type(java_type: str) -> bool:
    if java_type in {
        "Boolean",
        "Byte",
        "Double",
        "Float",
        "Integer",
        "Long",
        "Short",
        "String",
        "boolean",
        "byte",
        "double",
        "float",
        "int",
        "long",
        "short",
    }:
        return False
    return bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]*", java_type))


def _simple_type_name(java_type: str) -> str:
    return java_type.rsplit(".", maxsplit=1)[-1]
