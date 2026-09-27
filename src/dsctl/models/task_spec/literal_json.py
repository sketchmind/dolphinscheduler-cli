from __future__ import annotations

import json
from typing import TYPE_CHECKING, ClassVar, cast

from pydantic import (
    ConfigDict,
    Field,
    field_validator,
)
from pydantic_core import PydanticCustomError

from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)
from dsctl.models.task_spec.values import (
    contains_ds_parameter_placeholder,
)

if TYPE_CHECKING:
    from dsctl.models.common import (
        YamlObject,
        YamlValue,
    )

_LITERAL_JSON_ROOT_TYPE_ERROR = "literal_json_object_type"


def _parse_literal_json_number(_value: str) -> None:
    """Accept a valid JSON number token without imposing Python numeric limits."""


def _reject_literal_json_constant(value: str) -> YamlValue:
    """Reject Java-incompatible non-standard JSON numeric constants."""
    message = f"Non-standard JSON constant is unsupported: {value}"
    raise ValueError(message)


def _reject_literal_json_duplicate_keys(
    pairs: list[tuple[str, YamlValue]],
) -> YamlObject:
    """Build one JSON object while rejecting last-key-wins ambiguity."""
    result: YamlObject = {}
    for key, item in pairs:
        if key in result:
            message = f"Duplicate JSON object key is unsupported: {key}"
            raise ValueError(message)
        result[key] = item
    return result


def _literal_json_string_has_forbidden_codepoint(value: str) -> bool:
    """Recognize decoded controls and unpaired-surrogate code points."""
    return any(
        codepoint <= 0x1F or 0x7F <= codepoint <= 0x9F or 0xD800 <= codepoint <= 0xDFFF
        for codepoint in map(ord, value)
    )


def _validate_literal_json_decoded_strings(value: YamlValue) -> None:
    """Reject unsafe strings anywhere in one decoded literal job object."""
    pending = [value]
    while pending:
        current = pending.pop()
        if isinstance(current, str):
            if _literal_json_string_has_forbidden_codepoint(current):
                message = "json strings must not contain controls or surrogates"
                raise ValueError(message)
            if contains_ds_parameter_placeholder(current):
                message = "json strings must not contain DolphinScheduler placeholders"
                raise ValueError(message)
        elif isinstance(current, list):
            pending.extend(current)
        elif isinstance(current, dict):
            pending.extend(current.keys())
            pending.extend(current.values())


def _validate_literal_json_object(
    value: str,
    *,
    root_type_error: str = _LITERAL_JSON_ROOT_TYPE_ERROR,
) -> str:
    """Keep one literal JSON-object job without rewriting its spelling."""
    if not value.strip():
        message = "json must be one nonblank JSON object string"
        raise ValueError(message)
    if "\r" in value:
        message = "json must use LF rather than CR or CRLF line endings"
        raise ValueError(message)
    if contains_ds_parameter_placeholder(value):
        message = "json must not contain DolphinScheduler placeholders"
        raise ValueError(message)
    try:
        parsed = cast(
            "YamlValue",
            json.loads(
                value,
                parse_float=_parse_literal_json_number,
                parse_int=_parse_literal_json_number,
                parse_constant=_reject_literal_json_constant,
                object_pairs_hook=_reject_literal_json_duplicate_keys,
            ),
        )
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        message = "json must be one syntactically valid JSON object string"
        raise ValueError(message) from exc
    if not isinstance(parsed, dict):
        message = "json must decode to one JSON object"
        raise PydanticCustomError(root_type_error, message)
    _validate_literal_json_decoded_strings(parsed)
    return value


class _LiteralJsonObjectTaskParamsSpec(TaskParamsSpec):
    """Shared closed schema for one spelling-preserved JSON-object job."""

    json_root_type_error: ClassVar[str] = _LITERAL_JSON_ROOT_TYPE_ERROR

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=False,
        validate_by_alias=True,
        validate_by_name=False,
        strict=True,
        json_schema_extra={
            "x-dsctl-runtime-validations": [
                "json must decode to one JSON object",
                "non-standard numeric constants are rejected",
                "duplicate object keys are rejected at every nesting level",
                (
                    "decoded keys and values reject controls, surrogates, and "
                    "DolphinScheduler placeholders"
                ),
                "the original valid JSON spelling and LF formatting are preserved",
            ]
        },
    )

    json_text: str = Field(
        alias="json",
        min_length=1,
    )

    @field_validator("json_text")
    @classmethod
    def validate_json_text(cls, value: str) -> str:
        """Validate the literal job while retaining the authored JSON text."""
        return _validate_literal_json_object(
            value,
            root_type_error=cls.json_root_type_error,
        )
