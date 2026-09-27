from __future__ import annotations

import re

from pydantic import (
    ConfigDict,
    Field,
    field_validator,
)

from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)

_FLINK_INLINE_LOCAL_SQL_BLANK_CHARACTERS = (
    r"\x09\x0a\x0d\x20\x85\xa0\u1680\u2000-\u200a"
    r"\u2028\u2029\u202f\u205f\u3000\ufeff"
)
_FLINK_INLINE_LOCAL_SQL_BLANK_PATTERN = re.compile(
    rf"^[{_FLINK_INLINE_LOCAL_SQL_BLANK_CHARACTERS}]*$"
)
FLINK_INLINE_LOCAL_SQL_JSON_SCHEMA_PATTERN = (
    rf"^(?![{_FLINK_INLINE_LOCAL_SQL_BLANK_CHARACTERS}]*$)"
    r"(?![\s\S]*(?:\$\{|\$\[))"
    r"[^\x00-\x08\x0b-\x1f\x7f-\x9f\ud800-\udfff]*$"
)
FLINK_INLINE_LOCAL_SQL_ASCII_JSON_SCHEMA_PATTERN = (
    rf"^(?![{_FLINK_INLINE_LOCAL_SQL_BLANK_CHARACTERS}]*$)"
    r"(?![\s\S]*(?:\$\{|\$\[))"
    r"[\x09\x0a\x20-\x7e]*$"
)


def validate_flink_inline_local_sql(value: str) -> str:
    """Reject placeholders and controls without rewriting literal Flink SQL."""
    if _FLINK_INLINE_LOCAL_SQL_BLANK_PATTERN.fullmatch(value):
        message = "rawScript must not be blank"
        raise ValueError(message)
    if "${" in value or "$[" in value:
        message = "rawScript must not contain DolphinScheduler placeholders"
        raise ValueError(message)
    if any(
        (ord(character) < 0x20 and character not in "\t\n")
        or 0x7F <= ord(character) <= 0x9F
        or 0xD800 <= ord(character) <= 0xDFFF
        for character in value
    ):
        message = "rawScript contains an unsupported control or surrogate character"
        raise ValueError(message)
    return value


def validate_flink_inline_local_sql_ascii(value: str) -> str:
    """Refine Flink SQL to the portable 3.0.x default-charset subset."""
    validate_flink_inline_local_sql(value)
    if not value.isascii():
        message = "rawScript must contain only ASCII text on DolphinScheduler 3.0.x"
        raise ValueError(message)
    return value


class FlinkInlineLocalSqlTaskParamsSpec(TaskParamsSpec):
    """Portable literal SQL intent for one worker-local Flink SQL client."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    raw_script: str = Field(
        alias="rawScript",
        min_length=1,
        description=(
            "Nonblank literal Flink SQL preserved as UTF-8 on supported profiles; "
            "DolphinScheduler placeholders, carriage returns, and C0/C1 controls "
            "or unpaired Unicode surrogates are rejected."
        ),
        json_schema_extra={"pattern": FLINK_INLINE_LOCAL_SQL_JSON_SCHEMA_PATTERN},
    )

    @field_validator("raw_script")
    @classmethod
    def validate_raw_script(cls, value: str) -> str:
        """Keep one safe literal script in its original spelling."""
        return validate_flink_inline_local_sql(value)


class FlinkInlineLocalSqlAsciiTaskParamsSpec(FlinkInlineLocalSqlTaskParamsSpec):
    """ASCII refinement for the 3.0.x platform-default script writer."""

    raw_script: str = Field(
        alias="rawScript",
        min_length=1,
        description=(
            "Nonblank ASCII-only literal Flink SQL for DolphinScheduler 3.0.x, "
            "whose worker writes SQL with the platform-default charset; "
            "placeholders, carriage returns, C0/C1 controls, and Unicode "
            "surrogates are rejected."
        ),
        json_schema_extra={"pattern": FLINK_INLINE_LOCAL_SQL_ASCII_JSON_SCHEMA_PATTERN},
    )

    @field_validator("raw_script")
    @classmethod
    def validate_ascii_raw_script(cls, value: str) -> str:
        """Keep the exact cross-worker ASCII subset without normalization."""
        return validate_flink_inline_local_sql_ascii(value)
