from __future__ import annotations

import re
from typing import TYPE_CHECKING, Literal

from pydantic import (
    ConfigDict,
    Field,
    field_validator,
)

from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)

if TYPE_CHECKING:
    from dsctl.models.common import (
        YamlValue,
    )

_DMS_REPLICATION_TASK_ARN_UNSAFE_CHARACTERS = (
    r"\x00-\x20\x7f-\x9f\xa0\u1680\u2000-\u200a"
    r"\u2028\u2029\u202f\u205f\u3000\ufeff\ud800-\udfff"
)
DMS_REPLICATION_TASK_ARN_JSON_SCHEMA_PATTERN = (
    r"^arn:[A-Za-z0-9-]+:dms:[A-Za-z0-9-]+:[0-9]{12}:task:"
    r"(?![\s\S]*(?:\$\{|\$\[))"
    rf"[^{_DMS_REPLICATION_TASK_ARN_UNSAFE_CHARACTERS}]+(?![\s\S])"
)
_DMS_REPLICATION_TASK_ARN_PATTERN = re.compile(
    DMS_REPLICATION_TASK_ARN_JSON_SCHEMA_PATTERN
)


def validate_dms_replication_task_arn(value: str) -> str:
    """Accept one literal AWS DMS replication-task ARN without rewriting it."""
    if not _DMS_REPLICATION_TASK_ARN_PATTERN.fullmatch(value):
        message = "replicationTaskArn must be one literal AWS DMS task ARN"
        raise ValueError(message)
    return value


class DmsResumeExistingFullLoadTaskParamsSpec(TaskParamsSpec):
    """Exact intent for resuming one previously stopped full-load DMS task."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, strict=True)

    is_restart_task: Literal[True] = Field(alias="isRestartTask")
    is_json_format: Literal[False] = Field(alias="isJsonFormat")
    migration_type: Literal["full-load"] = Field(alias="migrationType")
    start_replication_task_type: Literal["resume-processing"] = Field(
        alias="startReplicationTaskType"
    )
    replication_task_arn: str = Field(
        alias="replicationTaskArn",
        min_length=1,
        json_schema_extra={"pattern": DMS_REPLICATION_TASK_ARN_JSON_SCHEMA_PATTERN},
    )

    @field_validator("is_restart_task", mode="before")
    @classmethod
    def validate_restart_constant(cls, value: YamlValue) -> YamlValue:
        """Reject integer truthiness and require the exact JSON boolean true."""
        if value is not True:
            message = "isRestartTask must be the strict boolean true"
            raise ValueError(message)
        return value

    @field_validator("is_json_format", mode="before")
    @classmethod
    def validate_json_format_constant(cls, value: YamlValue) -> YamlValue:
        """Reject integer falsiness and require the exact JSON boolean false."""
        if value is not False:
            message = "isJsonFormat must be the strict boolean false"
            raise ValueError(message)
        return value

    @field_validator("replication_task_arn")
    @classmethod
    def validate_replication_task_arn(cls, value: str) -> str:
        """Reject placeholders, non-task resources, and malformed identifiers."""
        return validate_dms_replication_task_arn(value)
