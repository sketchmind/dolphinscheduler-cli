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

_SPARK_INLINE_LOCAL_SQL_BLANK_CHARACTERS = (
    r"\x09\x0a\x0d\x20\x85\xa0\u1680\u2000-\u200a"
    r"\u2028\u2029\u202f\u205f\u3000\ufeff"
)
_SPARK_INLINE_LOCAL_SQL_BLANK_PATTERN = re.compile(
    rf"^[{_SPARK_INLINE_LOCAL_SQL_BLANK_CHARACTERS}]*$"
)
SPARK_INLINE_LOCAL_SQL_JSON_SCHEMA_PATTERN = (
    rf"^(?![{_SPARK_INLINE_LOCAL_SQL_BLANK_CHARACTERS}]*$)"
    r"(?![\s\S]*(?:\$\{|\$\[))"
    r"[^\x00-\x08\x0b\x0c\x0e-\x1f\x7f]*$"
)


def validate_spark_inline_local_sql(value: str) -> str:
    """Reject placeholders and unsafe controls without rewriting SQL."""
    if _SPARK_INLINE_LOCAL_SQL_BLANK_PATTERN.fullmatch(value):
        message = "rawScript must not be empty"
        raise ValueError(message)
    if "${" in value or "$[" in value:
        message = "rawScript must not contain DolphinScheduler placeholders"
        raise ValueError(message)
    if any(
        ord(character) == 0x7F or (ord(character) < 0x20 and character not in "\t\n\r")
        for character in value
    ):
        message = "rawScript contains an unsupported control character"
        raise ValueError(message)
    return value


class SparkInlineLocalSqlTaskParamsSpec(TaskParamsSpec):
    """Portable literal SQL intent for one worker-local Spark task."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    raw_script: str = Field(
        alias="rawScript",
        min_length=1,
        json_schema_extra={"pattern": SPARK_INLINE_LOCAL_SQL_JSON_SCHEMA_PATTERN},
    )

    @field_validator("raw_script")
    @classmethod
    def validate_raw_script(cls, value: str) -> str:
        """Keep one safe literal script in its original spelling."""
        return validate_spark_inline_local_sql(value)
