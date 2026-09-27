from __future__ import annotations

from enum import StrEnum

from pydantic import (
    Field,
    field_validator,
)

from dsctl.models.common import (
    GlobalParamSpec,
    YamlSpecModel,
)
from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)


class HttpRequestMethod(StrEnum):
    """Supported DS HTTP task methods."""

    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"


class HttpCheckCondition(StrEnum):
    """Supported DS HTTP task check conditions."""

    STATUS_CODE_DEFAULT = "STATUS_CODE_DEFAULT"
    STATUS_CODE_CUSTOM = "STATUS_CODE_CUSTOM"
    BODY_CONTAINS = "BODY_CONTAINS"
    BODY_NOT_CONTAINS = "BODY_NOT_CONTAINS"


class HttpParametersType(StrEnum):
    """Supported DS HTTP parameter kinds."""

    PARAMETER = "PARAMETER"
    HEADERS = "HEADERS"


class HttpPropertySpec(YamlSpecModel):
    """Typed YAML shape for one HTTP parameter/header entry."""

    prop: str
    http_parameters_type: HttpParametersType = Field(alias="httpParametersType")
    value: str

    @field_validator("prop", "value")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Reject empty HTTP property fields after trimming."""
        normalized = value.strip()
        if not normalized:
            message = "HTTP property fields must not be empty"
            raise ValueError(message)
        return normalized


class HttpTaskParamsSpec(TaskParamsSpec):
    """Typed YAML shape for HTTP task params."""

    url: str
    http_method: HttpRequestMethod = Field(alias="httpMethod")
    http_params: list[HttpPropertySpec] = Field(
        default_factory=list,
        alias="httpParams",
    )
    http_body: str | None = Field(default=None, alias="httpBody")
    http_check_condition: HttpCheckCondition = Field(
        default=HttpCheckCondition.STATUS_CODE_DEFAULT,
        alias="httpCheckCondition",
    )
    condition: str | None = None
    connect_timeout: int = Field(alias="connectTimeout", gt=0)
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )
    var_pool: list[GlobalParamSpec] = Field(default_factory=list, alias="varPool")

    @field_validator("url")
    @classmethod
    def validate_optional_text(cls, value: str | None) -> str | None:
        """Reject empty HTTP text fields after trimming."""
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            message = "Task text fields must not be empty"
            raise ValueError(message)
        return normalized
