from __future__ import annotations

import re

from pydantic import (
    ConfigDict,
    Field,
    field_validator,
)

from dsctl.models.common import GlobalParamSpec, YamlSpecModel
from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)

SCRIPT_RESOURCE_NAME_JSON_SCHEMA_PATTERN = r"^/[^/](?:[\s\S]*[^/])?$"
_SCRIPT_RESOURCE_NAME_PATTERN = re.compile(SCRIPT_RESOURCE_NAME_JSON_SCHEMA_PATTERN)


class ScriptResourceRefSpec(YamlSpecModel):
    """One canonical script FILE name, independent of native wire identity."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, strict=True)

    resource_name: str = Field(
        alias="resourceName",
        min_length=2,
        json_schema_extra={"pattern": SCRIPT_RESOURCE_NAME_JSON_SCHEMA_PATTERN},
        description=(
            "FILE-root-relative resource file path with one leading slash; "
            "the compiler resolves its exact native ID or storage path."
        ),
    )

    @field_validator("resource_name")
    @classmethod
    def validate_resource_name(cls, value: str) -> str:
        """Keep the documented rooted file name without path normalization."""
        if not _SCRIPT_RESOURCE_NAME_PATTERN.fullmatch(value):
            message = (
                "resourceName must be a FILE-root-relative file path with one "
                "leading slash and a nonempty file name"
            )
            raise ValueError(message)
        return value


class ScriptTaskParamsSpec(TaskParamsSpec):
    """Shared YAML shape for SHELL and PYTHON task params."""

    raw_script: str = Field(alias="rawScript")
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )
    resource_list: list[ScriptResourceRefSpec] = Field(
        default_factory=list,
        alias="resourceList",
    )
    var_pool: list[GlobalParamSpec] = Field(default_factory=list, alias="varPool")

    @field_validator("raw_script")
    @classmethod
    def validate_script(cls, value: str) -> str:
        """Reject blank raw script payloads."""
        if not value.strip():
            message = "rawScript must not be empty"
            raise ValueError(message)
        return value
