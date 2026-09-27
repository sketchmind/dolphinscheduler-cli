from __future__ import annotations

import re
from typing import TYPE_CHECKING, Annotated, cast

from pydantic import (
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
)
from pydantic_core import PydanticCustomError

from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)
from dsctl.models.task_spec.datasource_ref import DatasourceReference

if TYPE_CHECKING:
    from dsctl.models.common import (
        YamlValue,
    )

_ALIYUN_SERVERLESS_SPARK_EDGE_WHITESPACE = (
    r"\x09\x0a\x0b\x0c\x0d\x20\x85\xa0\u1680\u2000-\u200a"
    r"\u2028\u2029\u202f\u205f\u3000\ufeff"
)
ALIYUN_SERVERLESS_SPARK_LITERAL_JSON_SCHEMA_PATTERN = (
    rf"^(?=[\s\S])(?![{_ALIYUN_SERVERLESS_SPARK_EDGE_WHITESPACE}])"
    r"(?![\s\S]*(?:\$\{|\$\[))"
    rf"(?![\s\S]*[{_ALIYUN_SERVERLESS_SPARK_EDGE_WHITESPACE}](?![\s\S]))"
    r"[^\x00-\x1f\x7f-\x9f\ud800-\udfff]*(?![\s\S])"
)
ALIYUN_SERVERLESS_SPARK_ARGUMENT_JSON_SCHEMA_PATTERN = (
    rf"^(?=[\s\S])(?![{_ALIYUN_SERVERLESS_SPARK_EDGE_WHITESPACE}])"
    r"(?![\s\S]*(?:\$\{|\$\[))"
    rf"(?![\s\S]*[{_ALIYUN_SERVERLESS_SPARK_EDGE_WHITESPACE}](?![\s\S]))"
    r"[^#\x00-\x1f\x7f-\x9f\ud800-\udfff]*(?![\s\S])"
)
ALIYUN_SERVERLESS_SPARK_ENTRY_POINT_JSON_SCHEMA_PATTERN = (
    r"^oss://"
    r"(?![\s\S]*(?:\$\{|\$\[))"
    r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?/"
    rf"[^{_ALIYUN_SERVERLESS_SPARK_EDGE_WHITESPACE}"
    r"?#\x00-\x1f\x7f-\x9f\ud800-\udfff]+$"
)


_ALIYUN_SERVERLESS_SPARK_LITERAL_PATTERN = re.compile(
    ALIYUN_SERVERLESS_SPARK_LITERAL_JSON_SCHEMA_PATTERN
)
_ALIYUN_SERVERLESS_SPARK_ARGUMENT_PATTERN = re.compile(
    ALIYUN_SERVERLESS_SPARK_ARGUMENT_JSON_SCHEMA_PATTERN
)
_ALIYUN_SERVERLESS_SPARK_ENTRY_POINT_PATTERN = re.compile(
    ALIYUN_SERVERLESS_SPARK_ENTRY_POINT_JSON_SCHEMA_PATTERN
)
_ALIYUN_SERVERLESS_SPARK_PRODUCTION_TYPE_ERROR = (
    "aliyun_serverless_spark_production_type"
)
_ALIYUN_SERVERLESS_SPARK_LOGGED_VALUE_DESCRIPTION = (
    "Upstream logs the complete task_params object at INFO. This field is not "
    "secret storage, and the CLI does not detect or redact secrets."
)


def validate_aliyun_serverless_spark_literal(value: str, *, field: str) -> str:
    """Keep one literal remote request value without rewriting its spelling."""
    if not _ALIYUN_SERVERLESS_SPARK_LITERAL_PATTERN.fullmatch(value):
        message = (
            f"{field} must be one nonblank literal string without edge whitespace, "
            "controls, surrogates, or DS placeholders"
        )
        raise ValueError(message)
    return value


def validate_aliyun_serverless_spark_entry_point(value: str) -> str:
    """Accept one absolute, literal OSS object URI for a JAR entry point."""
    if not _ALIYUN_SERVERLESS_SPARK_ENTRY_POINT_PATTERN.fullmatch(value):
        message = "entryPoint must be one safe absolute oss:// JAR URI"
        raise ValueError(message)
    return value


def validate_aliyun_serverless_spark_argument(value: str) -> str:
    """Accept one unambiguous argument in the upstream hash-delimited wire."""
    if not _ALIYUN_SERVERLESS_SPARK_ARGUMENT_PATTERN.fullmatch(value):
        message = (
            "entryPointArguments items must be nonblank literal strings without "
            "edge whitespace, controls, surrogates, DS placeholders, or #"
        )
        raise ValueError(message)
    return value


AliyunServerlessSparkArgument = Annotated[
    str,
    Field(
        min_length=1,
        description=_ALIYUN_SERVERLESS_SPARK_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={
            "pattern": ALIYUN_SERVERLESS_SPARK_ARGUMENT_JSON_SCHEMA_PATTERN
        },
    ),
]


class AliyunServerlessSparkLiteralJarTaskParamsSpec(TaskParamsSpec):
    """Closed literal JAR submission intent for Aliyun Serverless Spark."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, strict=True)

    datasource: DatasourceReference = Field(
        description=(
            "Positive datasource id or exact datasource name. "
            + _ALIYUN_SERVERLESS_SPARK_LOGGED_VALUE_DESCRIPTION
        ),
    )
    workspace_id: str = Field(
        alias="workspaceId",
        min_length=1,
        description=_ALIYUN_SERVERLESS_SPARK_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={
            "pattern": ALIYUN_SERVERLESS_SPARK_LITERAL_JSON_SCHEMA_PATTERN
        },
    )
    resource_queue_id: str = Field(
        alias="resourceQueueId",
        min_length=1,
        description=_ALIYUN_SERVERLESS_SPARK_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={
            "pattern": ALIYUN_SERVERLESS_SPARK_LITERAL_JSON_SCHEMA_PATTERN
        },
    )
    job_name: str = Field(
        alias="jobName",
        min_length=1,
        description=_ALIYUN_SERVERLESS_SPARK_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={
            "pattern": ALIYUN_SERVERLESS_SPARK_LITERAL_JSON_SCHEMA_PATTERN
        },
    )
    entry_point: str = Field(
        alias="entryPoint",
        min_length=1,
        description=_ALIYUN_SERVERLESS_SPARK_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={
            "pattern": ALIYUN_SERVERLESS_SPARK_ENTRY_POINT_JSON_SCHEMA_PATTERN
        },
    )
    entry_point_arguments: list[AliyunServerlessSparkArgument] = Field(
        alias="entryPointArguments",
        min_length=1,
        description=_ALIYUN_SERVERLESS_SPARK_LOGGED_VALUE_DESCRIPTION,
    )
    spark_submit_parameters: str = Field(
        alias="sparkSubmitParameters",
        min_length=1,
        description=_ALIYUN_SERVERLESS_SPARK_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={
            "pattern": ALIYUN_SERVERLESS_SPARK_LITERAL_JSON_SCHEMA_PATTERN
        },
    )
    is_production: bool = Field(
        default=False,
        alias="isProduction",
        description=_ALIYUN_SERVERLESS_SPARK_LOGGED_VALUE_DESCRIPTION,
    )

    def default_payload_field_names(self) -> tuple[str, ...]:
        """Keep the exact upstream development-environment default explicit."""
        return ("is_production",)

    @field_validator(
        "workspace_id",
        "resource_queue_id",
        "job_name",
        "spark_submit_parameters",
    )
    @classmethod
    def validate_literal_field(cls, value: str, info: ValidationInfo) -> str:
        """Reject values whose spelling cannot be sent as literal intent."""
        field = {
            "workspace_id": "workspaceId",
            "resource_queue_id": "resourceQueueId",
            "job_name": "jobName",
            "spark_submit_parameters": "sparkSubmitParameters",
        }[cast("str", info.field_name)]
        return validate_aliyun_serverless_spark_literal(value, field=field)

    @field_validator("entry_point")
    @classmethod
    def validate_entry_point(cls, value: str) -> str:
        """Restrict typed JAR submission to one literal OSS object URI."""
        return validate_aliyun_serverless_spark_entry_point(value)

    @field_validator("entry_point_arguments", mode="before")
    @classmethod
    def validate_argument_collection(cls, value: YamlValue) -> YamlValue:
        """Require a non-empty JSON array before item-level validation."""
        if not isinstance(value, list) or not value:
            message = "entryPointArguments must be one non-empty strict list"
            raise ValueError(message)
        return value

    @field_validator("entry_point_arguments")
    @classmethod
    def validate_argument_items(cls, value: list[str]) -> list[str]:
        """Keep list-to-hash projection lossless in both directions."""
        return [validate_aliyun_serverless_spark_argument(item) for item in value]

    @field_validator("is_production", mode="before")
    @classmethod
    def validate_production_flag(cls, value: YamlValue) -> YamlValue:
        """Reject integer/string truthiness for the environment selector."""
        if not isinstance(value, bool):
            message = "isProduction must be one strict boolean"
            raise PydanticCustomError(
                _ALIYUN_SERVERLESS_SPARK_PRODUCTION_TYPE_ERROR,
                message,
            )
        return value
