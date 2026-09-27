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

PYTORCH_SCRIPT_RESOURCE_JSON_SCHEMA_PATTERN = (
    r"^/(?!-)(?!\.{1,2}(?:/|$))(?!.*//)(?!.*(?:/\.{1,2})(?:/|$))"
    r"[A-Za-z0-9_./:@%+=,\-]+\.py$"
)
PYTORCH_PYTHON_EXECUTABLE_JSON_SCHEMA_PATTERN = (
    r"^/(?!\.{1,2}(?:/|$))(?!.*//)(?!.*(?:/\.{1,2})(?:/|$))"
    r"[A-Za-z0-9_.:@%+=,\-]+(?:/[A-Za-z0-9_.:@%+=,\-]+)*$"
)
_PYTORCH_SCRIPT_RESOURCE_PATTERN = re.compile(
    PYTORCH_SCRIPT_RESOURCE_JSON_SCHEMA_PATTERN
)
_PYTORCH_PYTHON_EXECUTABLE_PATTERN = re.compile(
    PYTORCH_PYTHON_EXECUTABLE_JSON_SCHEMA_PATTERN
)
_PYTORCH_SCRIPT_ARGS_TYPE_ERROR = "pytorch_script_args_type"


def validate_pytorch_script_resource(value: str) -> str:
    """Accept one FILE-root-relative canonical Python resource path."""
    if not _PYTORCH_SCRIPT_RESOURCE_PATTERN.fullmatch(value):
        message = (
            "scriptResource must be one leading-slash, FILE-root-relative ASCII "
            "shell-safe path ending in .py without empty or dot traversal components"
        )
        raise ValueError(message)
    return value


def validate_pytorch_python_executable(value: str) -> str:
    """Accept one absolute literal shell-safe Python executable path."""
    if not _PYTORCH_PYTHON_EXECUTABLE_PATTERN.fullmatch(value):
        message = (
            "pythonExecutable must be one absolute ASCII shell-safe executable "
            "path without empty, trailing, or dot traversal components"
        )
        raise ValueError(message)
    return value


def validate_pytorch_argument(value: str) -> str:
    """Keep one Python argument safe in the upstream unquoted shell slot."""
    if not SHELL_ARGUMENT_PATTERN.fullmatch(value):
        message = (
            "scriptArgs items must be nonblank ASCII shell-safe tokens without "
            "whitespace, controls, expansion syntax, or DS placeholders"
        )
        raise ValueError(message)
    return value


_PYTORCH_LOGGED_VALUE_DESCRIPTION = (
    "Upstream logs the complete PYTORCH task params and generated shell command "
    "at INFO. This field is not secret storage, and the CLI does not detect or "
    "redact secrets."
)

PytorchArgument = Annotated[
    str,
    Field(
        min_length=1,
        description=_PYTORCH_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": SHELL_ARGUMENT_JSON_SCHEMA_PATTERN},
    ),
]


class PytorchLiteralResourceScriptTaskParamsSpec(TaskParamsSpec):
    """Closed portable intent for one downloaded literal Python script."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, strict=True)

    python_executable: str = Field(
        alias="pythonExecutable",
        min_length=1,
        description=_PYTORCH_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={
            "pattern": PYTORCH_PYTHON_EXECUTABLE_JSON_SCHEMA_PATTERN,
        },
    )
    script_resource: str = Field(
        alias="scriptResource",
        min_length=1,
        description=(
            "Leading-slash path relative to the selected user's DolphinScheduler "
            "FILE resource root, not a storage absolute fullName. "
            + _PYTORCH_LOGGED_VALUE_DESCRIPTION
        ),
        json_schema_extra={"pattern": PYTORCH_SCRIPT_RESOURCE_JSON_SCHEMA_PATTERN},
    )
    script_args: list[PytorchArgument] = Field(
        default_factory=list,
        alias="scriptArgs",
        description=_PYTORCH_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"default": []},
    )

    def default_payload_field_names(self) -> tuple[str, ...]:
        """Keep the canonical empty script-argument list explicit."""
        return ("script_args",)

    @field_validator("script_resource")
    @classmethod
    def validate_script_resource(cls, value: str) -> str:
        """Require one downloadable portable Python resource identity."""
        return validate_pytorch_script_resource(value)

    @field_validator("python_executable")
    @classmethod
    def validate_python_executable(cls, value: str) -> str:
        """Require one explicit worker Python executable rather than a directory."""
        return validate_pytorch_python_executable(value)

    @field_validator("script_args", mode="before")
    @classmethod
    def validate_script_args_collection(cls, value: YamlValue) -> YamlValue:
        """Reject coercible iterables before validating individual tokens."""
        if not isinstance(value, list):
            message = "scriptArgs must be one strict list of shell-safe tokens"
            raise PydanticCustomError(_PYTORCH_SCRIPT_ARGS_TYPE_ERROR, message)
        return value

    @field_validator("script_args")
    @classmethod
    def validate_script_args(cls, value: list[str]) -> list[str]:
        """Keep list-to-single-space projection reversible and injection-safe."""
        return [validate_pytorch_argument(item) for item in value]
