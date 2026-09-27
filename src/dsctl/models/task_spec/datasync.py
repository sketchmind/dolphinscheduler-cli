from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, cast

from pydantic import (
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pydantic_core import PydanticCustomError

from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)

if TYPE_CHECKING:
    from dsctl.models.common import (
        YamlObject,
        YamlValue,
    )

_DATASYNC_BLANK_CHARACTERS = (
    r"\x09\x0a\x0b\x0c\x0d\x20\x85\xa0\u1680\u2000-\u200a"
    r"\u2028\u2029\u202f\u205f\u3000\ufeff"
)
DATASYNC_LITERAL_JSON_SCHEMA_PATTERN = (
    rf"^(?![{_DATASYNC_BLANK_CHARACTERS}]*$)"
    r"(?![\s\S]*(?:\$\{|\$\[))"
    r"[^\x00-\x1f\x7f-\x9f\ud800-\udfff]*$"
)
_DATASYNC_LITERAL_PATTERN = re.compile(DATASYNC_LITERAL_JSON_SCHEMA_PATTERN)
_DATASYNC_RAW_JSON_OBJECT_TYPE_ERROR = "datasync_raw_json_object_type"
_DATASYNC_LOGGED_VALUE_DESCRIPTION = (
    "Upstream logs the complete DataSync task parameters at INFO; this field "
    "is not secret storage, and dsctl neither detects nor redacts secrets."
)


def validate_datasync_literal(value: str, *, field: str) -> str:
    """Keep one nonblank DataSync value literal without rewriting it."""
    if not _DATASYNC_LITERAL_PATTERN.fullmatch(value):
        message = (
            f"{field} must be one nonblank literal string without controls, "
            "surrogates, or DolphinScheduler placeholders"
        )
        raise ValueError(message)
    return value


def _reject_datasync_json_constant(value: str) -> YamlValue:
    """Reject non-standard JSON constants that Jackson does not accept."""
    message = f"Non-standard JSON constant is unsupported: {value}"
    raise ValueError(message)


def validate_datasync_raw_json(value: str) -> str:
    """Require one nonblank syntactically valid JSON object string."""
    if not value.strip():
        message = "json must be one nonblank JSON object string"
        raise ValueError(message)
    try:
        parsed = json.loads(value, parse_constant=_reject_datasync_json_constant)
    except (json.JSONDecodeError, ValueError) as exc:
        message = "json must be one syntactically valid JSON object string"
        raise ValueError(message) from exc
    if not isinstance(parsed, dict):
        message = "json must decode to one JSON object"
        raise PydanticCustomError(_DATASYNC_RAW_JSON_OBJECT_TYPE_ERROR, message)
    return value


def _datasync_task_params_json_schema(schema: YamlObject) -> None:
    """Expose the mode-conditional non-null strings that runtime enforces."""
    raw_properties = schema.get("properties")
    if not isinstance(raw_properties, dict):
        message = "DATASYNC task params JSON Schema lacks properties"
        raise TypeError(message)
    properties = cast("dict[str, object]", raw_properties)
    for field_name in (
        "name",
        "sourceLocationArn",
        "destinationLocationArn",
        "cloudWatchLogGroupArn",
        "json",
    ):
        raw_field = properties.get(field_name)
        if not isinstance(raw_field, dict):
            message = f"DATASYNC task params JSON Schema lacks {field_name}"
            raise TypeError(message)
        field_schema = cast("dict[str, object]", raw_field)
        field_schema.pop("anyOf", None)
        field_schema.pop("default", None)
        field_schema["type"] = "string"
        field_schema["minLength"] = 1
    schema["oneOf"] = [
        {
            "title": "normal",
            "properties": {
                "jsonFormat": {"const": False},
                "name": {"type": "string"},
                "sourceLocationArn": {"type": "string"},
                "destinationLocationArn": {"type": "string"},
                "cloudWatchLogGroupArn": {"type": "string"},
            },
            "required": [
                "name",
                "sourceLocationArn",
                "destinationLocationArn",
            ],
            "not": {"required": ["json"]},
        },
        {
            "title": "raw-json",
            "properties": {
                "jsonFormat": {"const": True},
                "json": {"type": "string"},
            },
            "required": ["jsonFormat", "json"],
            "not": {
                "anyOf": [
                    {"required": ["name"]},
                    {"required": ["sourceLocationArn"]},
                    {"required": ["destinationLocationArn"]},
                    {"required": ["cloudWatchLogGroupArn"]},
                ]
            },
        },
    ]
    schema["x-dsctl-runtime-validations"] = [
        "jsonFormat is a strict boolean and defaults to false",
        (
            "normal mode requires name, sourceLocationArn, and "
            "destinationLocationArn and forbids json"
        ),
        (
            "raw-json mode requires explicit jsonFormat=true plus one nonblank "
            "syntactically valid JSON object string and forbids normal-mode siblings"
        ),
    ]


class DatasyncTaskParamsSpec(TaskParamsSpec):
    """Exact public DataSync normal fields or explicit raw-JSON wrapper."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        strict=True,
        json_schema_extra=_datasync_task_params_json_schema,
    )

    json_format: bool = Field(
        default=False,
        alias="jsonFormat",
        strict=True,
        description=(
            "Strict public-mode selector. Omit or use false for the reviewed "
            "normal fields; explicit true selects the raw JSON wrapper."
        ),
    )
    name: str | None = Field(
        default=None,
        min_length=1,
        description=_DATASYNC_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": DATASYNC_LITERAL_JSON_SCHEMA_PATTERN},
    )
    source_location_arn: str | None = Field(
        default=None,
        alias="sourceLocationArn",
        min_length=1,
        description=_DATASYNC_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": DATASYNC_LITERAL_JSON_SCHEMA_PATTERN},
    )
    destination_location_arn: str | None = Field(
        default=None,
        alias="destinationLocationArn",
        min_length=1,
        description=_DATASYNC_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": DATASYNC_LITERAL_JSON_SCHEMA_PATTERN},
    )
    cloud_watch_log_group_arn: str | None = Field(
        default=None,
        alias="cloudWatchLogGroupArn",
        min_length=1,
        description=_DATASYNC_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": DATASYNC_LITERAL_JSON_SCHEMA_PATTERN},
    )
    json_text: str | None = Field(
        default=None,
        alias="json",
        min_length=1,
        description=(
            "Explicit raw public mode. The worker deserializes this string into "
            "its known DatasyncParameters model; it is not arbitrary AWS "
            "CreateTask passthrough. INFO-logged and not secret storage."
        ),
    )

    def default_payload_field_names(self) -> tuple[str, ...]:
        """Keep the DS-native normal/raw discriminator explicit on the wire."""
        return ("json_format",)

    @field_validator("name", "source_location_arn", "destination_location_arn")
    @classmethod
    def validate_required_normal_literal(
        cls,
        value: str | None,
        info: ValidationInfo,
    ) -> str | None:
        """Validate present normal-mode identity fields without coercion."""
        if value is None:
            return None
        alias = cls.model_fields[cast("str", info.field_name)].alias
        return validate_datasync_literal(value, field=alias or str(info.field_name))

    @field_validator("cloud_watch_log_group_arn")
    @classmethod
    def validate_optional_normal_literal(cls, value: str | None) -> str | None:
        """Validate the optional log-group ARN when normal mode authors it."""
        if value is None:
            return None
        return validate_datasync_literal(value, field="cloudWatchLogGroupArn")

    @field_validator("json_text")
    @classmethod
    def validate_json_text(cls, value: str | None) -> str | None:
        """Validate an authored raw JSON object without normalizing its spelling."""
        if value is None:
            return None
        return validate_datasync_raw_json(value)

    @model_validator(mode="after")
    def validate_public_mode(self) -> DatasyncTaskParamsSpec:
        """Close each public UI mode over exactly its active field set."""
        normal_fields = {
            "name",
            "source_location_arn",
            "destination_location_arn",
            "cloud_watch_log_group_arn",
        }
        if self.json_format:
            authored_normal = sorted(normal_fields.intersection(self.model_fields_set))
            if authored_normal:
                names = ", ".join(
                    type(self).model_fields[field].alias or field
                    for field in authored_normal
                )
                message = f"raw JSON mode forbids normal-mode fields: {names}"
                raise ValueError(message)
            if "json_text" not in self.model_fields_set or self.json_text is None:
                message = "raw JSON mode requires task_params.json"
                raise ValueError(message)
            return self

        if "json_text" in self.model_fields_set:
            message = "normal mode forbids task_params.json"
            raise ValueError(message)
        if (
            "cloud_watch_log_group_arn" in self.model_fields_set
            and self.cloud_watch_log_group_arn is None
        ):
            message = (
                "cloudWatchLogGroupArn must be omitted or one nonblank literal string"
            )
            raise ValueError(message)
        missing = [
            type(self).model_fields[field].alias or field
            for field in (
                "name",
                "source_location_arn",
                "destination_location_arn",
            )
            if field not in self.model_fields_set or getattr(self, field) is None
        ]
        if missing:
            message = f"normal mode requires fields: {', '.join(missing)}"
            raise ValueError(message)
        return self
