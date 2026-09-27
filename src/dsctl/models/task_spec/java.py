from __future__ import annotations

import re
from typing import TYPE_CHECKING, Annotated

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
    SHELL_ARGUMENT_JSON_SCHEMA_PATTERN,
    SHELL_ARGUMENT_PATTERN,
)

if TYPE_CHECKING:
    from dsctl.models.common import (
        YamlValue,
    )

JAVA_RESOURCE_NAME_JSON_SCHEMA_PATTERN = (
    r"^/(?!\.{1,2}(?:/|$))(?!.*//)(?!.*(?:/\.{1,2})(?:/|$))"
    r"[A-Za-z0-9_./:@%+=,\-]+\.jar$"
)
JAVA_ARGUMENT_JSON_SCHEMA_PATTERN = SHELL_ARGUMENT_JSON_SCHEMA_PATTERN
_JAVA_RESOURCE_NAME_PATTERN = re.compile(JAVA_RESOURCE_NAME_JSON_SCHEMA_PATTERN)

_JAVA_MAIN_ARGS_TYPE_ERROR = "java_main_args_type"
_JAVA_LOGGED_VALUE_DESCRIPTION = (
    "Upstream logs the complete JAVA task params and final shell command at INFO. "
    "This field is not secret storage, and the CLI does not detect or redact secrets."
)


def validate_java_resource_name(value: str) -> str:
    """Accept one absolute, shell-safe DolphinScheduler JAR resource fullName."""
    if not _JAVA_RESOURCE_NAME_PATTERN.fullmatch(value):
        message = (
            "mainJar must be one absolute shell-safe DS resource fullName ending "
            "in .jar without empty or dot traversal components"
        )
        raise ValueError(message)
    return value


def validate_java_argument(value: str) -> str:
    """Keep one application argument safe in the upstream unquoted shell slot."""
    if not SHELL_ARGUMENT_PATTERN.fullmatch(value):
        message = (
            "mainArgs items must be nonblank shell-safe tokens without whitespace, "
            "controls, expansion syntax, or DS placeholders"
        )
        raise ValueError(message)
    return value


JavaArgument = Annotated[
    str,
    Field(
        min_length=1,
        description=_JAVA_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": JAVA_ARGUMENT_JSON_SCHEMA_PATTERN},
    ),
]


class JavaLiteralFatJarTaskParamsSpec(TaskParamsSpec):
    """Closed portable intent for one literal executable fat JAR."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, strict=True)

    main_jar: str = Field(
        alias="mainJar",
        min_length=1,
        description=_JAVA_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": JAVA_RESOURCE_NAME_JSON_SCHEMA_PATTERN},
    )
    main_args: list[JavaArgument] = Field(
        default_factory=list,
        alias="mainArgs",
        description=_JAVA_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"default": []},
    )

    def default_payload_field_names(self) -> tuple[str, ...]:
        """Keep the canonical empty application-argument list explicit."""
        return ("main_args",)

    @field_validator("main_jar")
    @classmethod
    def validate_main_jar(cls, value: str) -> str:
        """Require one downloadable portable JAR resource identity."""
        return validate_java_resource_name(value)

    @field_validator("main_args", mode="before")
    @classmethod
    def validate_main_args_collection(cls, value: YamlValue) -> YamlValue:
        """Reject coercible iterables before validating individual tokens."""
        if not isinstance(value, list):
            message = "mainArgs must be one strict list of shell-safe tokens"
            raise PydanticCustomError(_JAVA_MAIN_ARGS_TYPE_ERROR, message)
        return value

    @field_validator("main_args")
    @classmethod
    def validate_main_args(cls, value: list[str]) -> list[str]:
        """Keep list-to-single-space projection reversible and injection-safe."""
        return [validate_java_argument(item) for item in value]
