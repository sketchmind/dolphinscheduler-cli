from __future__ import annotations

import re
from typing import TYPE_CHECKING, Annotated, Literal

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
    UNICODE_EDGE_WHITESPACE_PATTERN,
)

if TYPE_CHECKING:
    from dsctl.models.common import (
        YamlValue,
    )

_SQOOP_PASSWORD_SWITCH_JSON_SCHEMA_GUARD = (
    r"(?!(?:-P|--password(?:=[\s\S]*)?)(?![\s\S]))"  # noqa: S105
)
SQOOP_ARGUMENT_JSON_SCHEMA_PATTERN = (
    rf"^{_SQOOP_PASSWORD_SWITCH_JSON_SCHEMA_GUARD}"
    rf"(?![{UNICODE_EDGE_WHITESPACE_PATTERN}])"
    r"(?![\s\S]*(?:\$\{|\$\[))"
    rf"(?![\s\S]*[{UNICODE_EDGE_WHITESPACE_PATTERN}](?![\s\S]))"
    r"[^\x00-\x1f\x7f-\x9f\ud800-\udfff]*(?![\s\S])"
)
SQOOP_ASCII_ARGUMENT_JSON_SCHEMA_PATTERN = (
    rf"^{_SQOOP_PASSWORD_SWITCH_JSON_SCHEMA_GUARD}"
    r"(?!\x20)(?![\x20-\x7e]*(?:\$\{|\$\[))"
    r"(?:[\x20-\x7e]*[\x21-\x7e])?$"
)
_SQOOP_ARGUMENT_PATTERN = re.compile(SQOOP_ARGUMENT_JSON_SCHEMA_PATTERN)
_SQOOP_ARGS_TYPE_ERROR = "sqoop_args_type"


_SQOOP_LOGGED_VALUE_DESCRIPTION = (
    "Upstream logs the complete SQOOP task params and generated command at INFO. "
    "This field is not secret storage; use a password file or another worker-side "
    "credential mechanism instead of an inline password."
)


def validate_sqoop_argument(value: str) -> str:
    """Keep one literal argument safe for compiler-owned POSIX shell quoting."""
    if value in {"-P", "--password"} or value.startswith("--password="):
        message = (
            "args must not request an interactive or inline password; use "
            "--password-file with worker-accessible credential storage"
        )
        raise ValueError(message)
    if not _SQOOP_ARGUMENT_PATTERN.fullmatch(value):
        message = (
            "args items must be literal strings without edge whitespace, "
            "controls, surrogates, or DS placeholders"
        )
        raise ValueError(message)
    return value


def validate_sqoop_argument_ascii(value: str) -> str:
    """Refine SQOOP arguments for platform-default script writers."""
    validate_sqoop_argument(value)
    if not value.isascii():
        message = (
            "args items must contain only ASCII text when the worker writes the "
            "SQOOP script with its platform-default charset"
        )
        raise ValueError(message)
    return value


SqoopArgument = Annotated[
    str,
    Field(
        description=_SQOOP_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": SQOOP_ARGUMENT_JSON_SCHEMA_PATTERN},
    ),
]
SqoopAsciiArgument = Annotated[
    str,
    Field(
        description=(
            _SQOOP_LOGGED_VALUE_DESCRIPTION
            + " This exact profile is restricted to ASCII because the worker "
            "writes the generated script with its platform-default charset."
        ),
        json_schema_extra={"pattern": SQOOP_ASCII_ARGUMENT_JSON_SCHEMA_PATTERN},
    ),
]


class SqoopLiteralCommandTaskParamsSpec(TaskParamsSpec):
    """Closed intent for one literal Sqoop import or export command."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, strict=True)

    subcommand: Literal["import", "export"]
    args: list[SqoopArgument] = Field(
        min_length=1,
        description=_SQOOP_LOGGED_VALUE_DESCRIPTION,
    )

    @field_validator("args", mode="before")
    @classmethod
    def validate_args_collection(cls, value: YamlValue) -> YamlValue:
        """Reject coercible iterables before validating ordered arguments."""
        if not isinstance(value, list):
            message = "args must be one nonempty strict list of literal arguments"
            raise PydanticCustomError(_SQOOP_ARGS_TYPE_ERROR, message)
        return value

    @field_validator("args")
    @classmethod
    def validate_args(cls, value: list[str]) -> list[str]:
        """Reject placeholders, controls, and credential-bearing switches."""
        return [validate_sqoop_argument(item) for item in value]


class SqoopLiteralCommandAsciiTaskParamsSpec(SqoopLiteralCommandTaskParamsSpec):
    """ASCII refinement for platform-default SQOOP script writers."""

    args: list[SqoopAsciiArgument] = Field(
        min_length=1,
        description=(
            _SQOOP_LOGGED_VALUE_DESCRIPTION
            + " Arguments are ASCII-only on this exact profile because the worker "
            "uses its platform-default script charset."
        ),
    )

    @field_validator("args")
    @classmethod
    def validate_ascii_args(cls, value: list[str]) -> list[str]:
        """Keep command spelling stable across worker default charsets."""
        return [validate_sqoop_argument_ascii(item) for item in value]
