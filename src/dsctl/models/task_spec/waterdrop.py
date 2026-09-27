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

WATERDROP_CONFIG_RESOURCE_JSON_SCHEMA_PATTERN = (
    r"^/(?!\.{1,2}(?:/|$))(?!.*//)(?!.*(?:/\.{1,2})(?:/|$))"
    r"[A-Za-z0-9_.:@%+=,\-]+(?:/[A-Za-z0-9_.:@%+=,\-]+)*$"
)
_WATERDROP_CONFIG_RESOURCE_PATTERN = re.compile(
    WATERDROP_CONFIG_RESOURCE_JSON_SCHEMA_PATTERN
)


def validate_waterdrop_config_resource(value: str) -> str:
    """Accept one absolute DS resource whose staged relative path is shell-safe."""
    if not _WATERDROP_CONFIG_RESOURCE_PATTERN.fullmatch(value):
        message = (
            "configResource must be one absolute ASCII shell-safe DS resource "
            "fullName without empty, trailing, or dot traversal components"
        )
        raise ValueError(message)
    return value


_WATERDROP_LOGGED_VALUE_DESCRIPTION = (
    "Upstream logs the complete WATERDROP task params and generated shell command "
    "at INFO. This field is not secret storage."
)


class WaterdropLiteralLocalConfigTaskParamsSpec(TaskParamsSpec):
    """Closed intent for one worker-local Waterdrop configuration job."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, strict=True)

    config_resource: str = Field(
        alias="configResource",
        min_length=2,
        description=_WATERDROP_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={
            "pattern": WATERDROP_CONFIG_RESOURCE_JSON_SCHEMA_PATTERN,
        },
    )

    @field_validator("config_resource")
    @classmethod
    def validate_config_resource(cls, value: str) -> str:
        """Keep the UI-compatible staged relative path literal and shell-safe."""
        return validate_waterdrop_config_resource(value)
