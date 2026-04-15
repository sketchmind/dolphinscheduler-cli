"""Shared metadata helpers for controller and type extraction."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

import javalang

from ds_codegen.extract.type_lookup import _render_type
from ds_codegen.java_literals import decode_java_literal_text

if TYPE_CHECKING:
    from ds_codegen.ir import AnnotationValue, ScalarValue


@dataclass(frozen=True)
class _ParsedDocComment:
    description: str | None
    params: dict[str, str]
    returns: str | None


@dataclass(frozen=True)
class _ParameterAnnotationMetadata:
    wire_name: str | None
    required: bool | None
    description: str | None
    example: str | None
    allowable_values: str | None
    schema_type: str | None


def _parse_doc_comment(raw_text: str | None) -> _ParsedDocComment:
    cleaned_lines = _clean_doc_comment_lines(raw_text)
    if not cleaned_lines:
        return _ParsedDocComment(
            description=None,
            params={},
            returns=None,
        )

    description_lines: list[str] = []
    parameter_docs: dict[str, str] = {}
    returns_lines: list[str] = []
    current_tag: Literal["description", "param", "return"] = "description"
    current_param_name: str | None = None
    current_tag_lines: list[str] = []

    def flush_current_tag() -> None:
        nonlocal current_tag, current_param_name, current_tag_lines, returns_lines
        text = _normalize_doc_lines(current_tag_lines)
        if (
            current_tag == "param"
            and current_param_name is not None
            and text is not None
        ):
            parameter_docs[current_param_name] = text
        elif current_tag == "return" and text is not None:
            returns_lines = [text]
        current_tag = "description"
        current_param_name = None
        current_tag_lines = []

    for line in cleaned_lines:
        if line.startswith("@param "):
            flush_current_tag()
            remainder = line[len("@param ") :].strip()
            param_name, _, description = remainder.partition(" ")
            current_tag = "param"
            current_param_name = param_name or None
            current_tag_lines = [description.strip()] if description.strip() else []
            continue
        if line.startswith("@return"):
            flush_current_tag()
            remainder = line[len("@return") :].strip()
            current_tag = "return"
            current_tag_lines = [remainder] if remainder else []
            continue
        if line.startswith("@"):
            flush_current_tag()
            continue
        if current_tag == "description":
            description_lines.append(line)
        else:
            current_tag_lines.append(line)

    flush_current_tag()
    return _ParsedDocComment(
        description=_normalize_doc_lines(description_lines),
        params=parameter_docs,
        returns=_normalize_doc_lines(returns_lines),
    )


def _clean_doc_comment_lines(raw_text: str | None) -> list[str]:
    if raw_text is None:
        return []
    text = raw_text.strip()
    text = text.removeprefix("/**")
    text = text.removesuffix("*/")
    cleaned_lines: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line.startswith("*"):
            line = line[1:].lstrip()
        cleaned_lines.append(line)
    return cleaned_lines


def _normalize_doc_lines(lines: list[str]) -> str | None:
    stripped_lines = [line.rstrip() for line in lines]
    while stripped_lines and not stripped_lines[0]:
        stripped_lines.pop(0)
    while stripped_lines and not stripped_lines[-1]:
        stripped_lines.pop()
    if not stripped_lines:
        return None

    paragraphs: list[str] = []
    current_paragraph: list[str] = []
    for line in stripped_lines:
        if not line:
            if current_paragraph:
                paragraphs.append(" ".join(current_paragraph).strip())
                current_paragraph = []
            continue
        current_paragraph.append(line.strip())
    if current_paragraph:
        paragraphs.append(" ".join(current_paragraph).strip())
    normalized = "\n\n".join(paragraph for paragraph in paragraphs if paragraph).strip()
    return normalized or None


def _normalize_annotation_doc_text(raw_text: str | None) -> str | None:
    if raw_text is None:
        return None
    normalized = raw_text.strip()
    if not normalized:
        return None
    if re.fullmatch(r"[A-Z][A-Z0-9_ -]*", normalized):
        return None
    return normalized


def _extract_method_parameter_metadata(
    annotations: list[javalang.tree.Annotation],
) -> dict[str, _ParameterAnnotationMetadata]:
    metadata_by_name: dict[str, _ParameterAnnotationMetadata] = {}
    for annotation in annotations:
        if annotation.name != "Parameters":
            continue
        if not isinstance(annotation.element, javalang.tree.ElementArrayValue):
            continue
        for element in annotation.element.values:
            if not isinstance(element, javalang.tree.Annotation):
                continue
            parameter_metadata = _extract_parameter_annotation_metadata(element)
            if parameter_metadata.wire_name is None:
                continue
            existing_metadata = metadata_by_name.get(parameter_metadata.wire_name)
            metadata_by_name[parameter_metadata.wire_name] = (
                _merge_parameter_annotation_metadata(
                    existing_metadata,
                    parameter_metadata,
                )
                if existing_metadata is not None
                else parameter_metadata
            )
    return metadata_by_name


def _extract_parameter_annotation_metadata(
    annotation: javalang.tree.Annotation,
) -> _ParameterAnnotationMetadata:
    if annotation.name != "Parameter":
        return _ParameterAnnotationMetadata(
            wire_name=None,
            required=None,
            description=None,
            example=None,
            allowable_values=None,
            schema_type=None,
        )
    values = _annotation_values(annotation)
    schema_metadata = _extract_schema_annotation_metadata(
        _find_nested_annotation(annotation, "schema")
    )
    return _ParameterAnnotationMetadata(
        wire_name=(
            _get_string_value(values, "name") or _get_string_value(values, "value")
        ),
        required=_get_bool_value(values, "required"),
        description=_normalize_annotation_doc_text(
            _get_string_value(values, "description")
        ),
        example=_get_string_value(values, "example") or schema_metadata.example,
        allowable_values=schema_metadata.allowable_values,
        schema_type=schema_metadata.schema_type,
    )


def _merge_parameter_annotation_metadata(
    primary: _ParameterAnnotationMetadata,
    secondary: _ParameterAnnotationMetadata,
) -> _ParameterAnnotationMetadata:
    return _ParameterAnnotationMetadata(
        wire_name=primary.wire_name or secondary.wire_name,
        required=(
            primary.required if primary.required is not None else secondary.required
        ),
        description=primary.description or secondary.description,
        example=primary.example or secondary.example,
        allowable_values=primary.allowable_values or secondary.allowable_values,
        schema_type=primary.schema_type or secondary.schema_type,
    )


def _find_nested_annotation(
    annotation: javalang.tree.Annotation,
    key: str,
) -> javalang.tree.Annotation | None:
    if not isinstance(annotation.element, list):
        return None
    for pair in annotation.element:
        if pair.name != key:
            continue
        if isinstance(pair.value, javalang.tree.Annotation):
            return pair.value
    return None


def _extract_schema_annotation_metadata(
    annotation: javalang.tree.Annotation | None,
) -> _ParameterAnnotationMetadata:
    if annotation is None or annotation.name != "Schema":
        return _ParameterAnnotationMetadata(
            wire_name=None,
            required=None,
            description=None,
            example=None,
            allowable_values=None,
            schema_type=None,
        )
    values = _annotation_values(annotation)
    return _ParameterAnnotationMetadata(
        wire_name=None,
        required=_get_bool_value(values, "required"),
        description=_normalize_annotation_doc_text(
            _get_string_value(values, "description")
        ),
        example=_get_string_value(values, "example"),
        allowable_values=_get_annotation_text_value(values, "allowableValues"),
        schema_type=_get_string_value(values, "implementation"),
    )


def _find_annotation_values(
    annotations: list[javalang.tree.Annotation],
    name: str,
) -> dict[str, AnnotationValue]:
    for annotation in annotations:
        if annotation.name == name:
            return _annotation_values(annotation)
    return {}


def _annotation_values(
    annotation: javalang.tree.Annotation,
) -> dict[str, AnnotationValue]:
    if annotation.element is None:
        return {}
    if isinstance(annotation.element, list):
        result: dict[str, AnnotationValue] = {}
        for pair in annotation.element:
            result[pair.name] = _decode_value(pair.value)
        return result
    return {"value": _decode_value(annotation.element)}


def _decode_value(value: object) -> AnnotationValue:
    if isinstance(value, javalang.tree.ElementArrayValue):
        return [_decode_scalar(sub_value) for sub_value in value.values]
    return _decode_scalar(value)


def _decode_scalar(value: object) -> ScalarValue:
    if isinstance(value, javalang.tree.Literal):
        return decode_java_literal_text(
            str(value.value),
            prefix_operators=value.prefix_operators,
        )
    if isinstance(value, javalang.tree.MemberReference):
        if value.qualifier:
            return f"{value.qualifier}.{value.member}"
        return cast("str", value.member)
    if isinstance(value, javalang.tree.ClassReference):
        return _render_type(value.type)
    return str(value)


def _get_bool_value(
    values: dict[str, AnnotationValue],
    key: str,
) -> bool | None:
    value = values.get(key)
    return value if isinstance(value, bool) else None


def _get_string_value(
    values: dict[str, AnnotationValue],
    key: str,
) -> str | None:
    value = values.get(key)
    return value if isinstance(value, str) else None


def _get_string_list_value(
    values: dict[str, AnnotationValue],
    key: str,
) -> list[str]:
    value = values.get(key)
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _get_annotation_text_value(
    values: dict[str, AnnotationValue],
    key: str,
) -> str | None:
    value = values.get(key)
    if isinstance(value, list):
        rendered_values = [_value_to_string(item) for item in value]
        return ", ".join(rendered_values) if rendered_values else None
    if value is None:
        return None
    return _value_to_string(value)


def _value_to_string(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
