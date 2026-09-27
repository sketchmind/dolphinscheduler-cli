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
from dsctl.models.task_spec.java import (
    JAVA_ARGUMENT_JSON_SCHEMA_PATTERN,
    JAVA_RESOURCE_NAME_JSON_SCHEMA_PATTERN,
    validate_java_argument,
    validate_java_resource_name,
)

if TYPE_CHECKING:
    from dsctl.models.common import (
        YamlValue,
    )

MR_MAIN_CLASS_JSON_SCHEMA_PATTERN = (
    r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$"
)
_MR_MAIN_CLASS_PATTERN = re.compile(MR_MAIN_CLASS_JSON_SCHEMA_PATTERN)
_MR_MAIN_ARGS_TYPE_ERROR = "mr_main_args_type"


def validate_mr_main_class(value: str) -> str:
    """Keep one unquoted Java entry class shell-safe and literal."""
    if not _MR_MAIN_CLASS_PATTERN.fullmatch(value):
        message = (
            "mainClass must be one literal ASCII Java class name without "
            "whitespace, shell expansion, or DS placeholders"
        )
        raise ValueError(message)
    return value


_MR_LOGGED_VALUE_DESCRIPTION = (
    "Upstream logs the complete MR task params and final shell command at INFO. "
    "This field is not secret storage, and the CLI does not detect or redact secrets."
)

MrArgument = Annotated[
    str,
    Field(
        min_length=1,
        description=_MR_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": JAVA_ARGUMENT_JSON_SCHEMA_PATTERN},
    ),
]


class MrLiteralJavaJarTaskParamsSpec(TaskParamsSpec):
    """Closed portable intent for one literal Java MapReduce JAR job."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, strict=True)

    main_jar: str = Field(
        alias="mainJar",
        min_length=1,
        description=_MR_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": JAVA_RESOURCE_NAME_JSON_SCHEMA_PATTERN},
    )
    main_class: str = Field(
        alias="mainClass",
        min_length=1,
        description=_MR_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": MR_MAIN_CLASS_JSON_SCHEMA_PATTERN},
    )
    main_args: list[MrArgument] = Field(
        default_factory=list,
        alias="mainArgs",
        description=_MR_LOGGED_VALUE_DESCRIPTION,
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

    @field_validator("main_class")
    @classmethod
    def validate_main_class(cls, value: str) -> str:
        """Keep the unquoted Java entry class shell-safe and literal."""
        return validate_mr_main_class(value)

    @field_validator("main_args", mode="before")
    @classmethod
    def validate_main_args_collection(cls, value: YamlValue) -> YamlValue:
        """Reject coercible iterables before validating individual tokens."""
        if not isinstance(value, list):
            message = "mainArgs must be one strict list of shell-safe tokens"
            raise PydanticCustomError(_MR_MAIN_ARGS_TYPE_ERROR, message)
        return value

    @field_validator("main_args")
    @classmethod
    def validate_main_args(cls, value: list[str]) -> list[str]:
        """Keep list-to-single-space projection reversible and injection-safe."""
        return [validate_java_argument(item) for item in value]
