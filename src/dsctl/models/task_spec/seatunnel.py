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

SEATUNNEL_LITERAL_CONFIG_JSON_SCHEMA_PATTERN = (
    r"^(?=[\s\S]*[\x21-\x7e])"
    r"(?![\s\S]*(?:\$\{|\$\[))"
    r"[\x09\x0a\x20-\x7e]+$"
)
_SEATUNNEL_LITERAL_CONFIG_PATTERN = re.compile(
    SEATUNNEL_LITERAL_CONFIG_JSON_SCHEMA_PATTERN
)


def validate_seatunnel_literal_config(value: str) -> str:
    """Keep one cross-version SeaTunnel configuration literal and ASCII-safe."""
    if not _SEATUNNEL_LITERAL_CONFIG_PATTERN.fullmatch(value):
        message = (
            "rawScript must contain nonblank literal ASCII SeaTunnel config with "
            "LF/tab only, without controls, Unicode, CR, or DS placeholders"
        )
        raise ValueError(message)
    return value


_SEATUNNEL_LOGGED_VALUE_DESCRIPTION = (
    "Upstream logs complete SEATUNNEL task parameters, generated commands, and "
    "custom configuration content at INFO. This field is not secret storage."
)


class SeatunnelLiteralLocalConfigTaskParamsSpec(TaskParamsSpec):
    """Closed cross-version intent for one worker-local SeaTunnel config job."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, strict=True)

    raw_script: str = Field(
        alias="rawScript",
        min_length=1,
        description=_SEATUNNEL_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={
            "pattern": SEATUNNEL_LITERAL_CONFIG_JSON_SCHEMA_PATTERN,
        },
    )

    @field_validator("raw_script")
    @classmethod
    def validate_raw_script(cls, value: str) -> str:
        """Preserve literal spelling while rejecting unsafe runtime expansion."""
        return validate_seatunnel_literal_config(value)
