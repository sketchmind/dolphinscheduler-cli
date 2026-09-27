from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from pydantic import (
    Field,
    field_validator,
)

from dsctl.models.common import GlobalParamSpec
from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)
from dsctl.models.task_spec.datasource_ref import DatasourceReference

if TYPE_CHECKING:
    from dsctl.models.common import (
        YamlValue,
    )


class RemoteShellTaskParamsSpec(TaskParamsSpec):
    """Typed YAML shape for REMOTESHELL task params."""

    raw_script: str = Field(alias="rawScript")
    remote_type: Literal["SSH"] = Field(default="SSH", alias="type")
    datasource: DatasourceReference = Field(
        description="Positive SSH datasource id or exact datasource name."
    )
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )
    var_pool: list[GlobalParamSpec] = Field(default_factory=list, alias="varPool")

    def default_payload_field_names(self) -> tuple[str, ...]:
        """Send the DS-native SSH type when YAML relies on the local default."""
        return ("remote_type",)

    @field_validator("raw_script")
    @classmethod
    def validate_raw_script(cls, value: str) -> str:
        """Reject blank REMOTESHELL scripts while preserving exact content."""
        if not value.strip():
            message = "rawScript must not be empty"
            raise ValueError(message)
        return value

    @field_validator("remote_type", mode="before")
    @classmethod
    def validate_remote_type(cls, value: YamlValue) -> YamlValue:
        """Normalize the only connection type supported by DS REMOTESHELL."""
        if not isinstance(value, str):
            return value
        normalized = value.strip().upper()
        if normalized != "SSH":
            message = "type must be SSH"
            raise ValueError(message)
        return normalized
