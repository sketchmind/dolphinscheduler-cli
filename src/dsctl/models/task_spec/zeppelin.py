from __future__ import annotations

import re
from collections.abc import Mapping
from enum import StrEnum
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

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
from dsctl.models.task_spec.datasource_ref import DatasourceReference
from dsctl.models.task_spec.values import (
    contains_ds_parameter_placeholder,
)

if TYPE_CHECKING:
    from dsctl.models.common import (
        YamlObject,
        YamlValue,
    )


class ZeppelinConnectionMode(StrEnum):
    """Stable connection intent projected onto exact Zeppelin task epochs."""

    WORKER_CONFIG = "WORKER_CONFIG"
    REST_ENDPOINT = "REST_ENDPOINT"
    DATASOURCE = "DATASOURCE"


ZEPPELIN_ID_JSON_SCHEMA_PATTERN = r"^(?!\.{1,2}$)[A-Za-z0-9._~-]+$"
ZEPPELIN_PARAMETER_KEY_JSON_SCHEMA_PATTERN = r"^[A-Za-z0-9_.-]+$"
ZEPPELIN_PARAMETER_VALUE_JSON_SCHEMA_PATTERN = r"^(?!.*(?:\$\{|\$\[))[^\x00-\x1f\x7f]*$"
ZEPPELIN_REST_ENDPOINT_JSON_SCHEMA_PATTERN = (
    r"^https?://(?!.*(?:\$\{|\$\[))"
    r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?"
    r"(?::(?:[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|"
    r"65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5]))?"
    r"(?:/[^\s?#\x00-\x1f\x7f]*)?$"
)
_ZEPPELIN_ID_PATTERN = re.compile(ZEPPELIN_ID_JSON_SCHEMA_PATTERN)
_ZEPPELIN_PARAMETER_KEY_PATTERN = re.compile(
    ZEPPELIN_PARAMETER_KEY_JSON_SCHEMA_PATTERN,
)
_ZEPPELIN_REST_ENDPOINT_PATTERN = re.compile(
    ZEPPELIN_REST_ENDPOINT_JSON_SCHEMA_PATTERN,
)
_ZEPPELIN_CONTROL_PATTERN = re.compile(r"[\x00-\x1f\x7f]")
_ZEPPELIN_PARAMETERS_TYPE_ERROR = "zeppelin_parameters_type"
_ZEPPELIN_PARAMETER_VALUE_TYPE_ERROR = "zeppelin_parameter_value_type"
_ZEPPELIN_CONDITIONAL_FIELD_ALIASES = {
    "rest_endpoint": "restEndpoint",
    "datasource": "datasource",
}


class ZeppelinParagraphTaskParamsSpec(TaskParamsSpec):
    """Portable paragraph-only intent across exact Zeppelin connection epochs."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    note_id: str = Field(alias="noteId")
    paragraph_id: str = Field(alias="paragraphId")
    connection_mode: ZeppelinConnectionMode = Field(alias="connectionMode")
    rest_endpoint: str | None = Field(default=None, alias="restEndpoint")
    datasource: DatasourceReference | None = Field(
        default=None,
        description="Positive ZEPPELIN datasource id or exact datasource name.",
    )
    parameters: dict[str, str] = Field(
        default_factory=dict,
        json_schema_extra={"default": {}},
    )

    def default_payload_field_names(self) -> tuple[str, ...]:
        """Keep the stable empty parameter map visible in every exact profile."""
        return ("parameters",)

    @field_validator("note_id", "paragraph_id")
    @classmethod
    def validate_zeppelin_id(cls, value: str, info: ValidationInfo) -> str:
        """Keep note and paragraph identities to one safe URL path segment."""
        if value in {".", ".."} or not _ZEPPELIN_ID_PATTERN.fullmatch(value):
            alias = "noteId" if info.field_name == "note_id" else "paragraphId"
            message = f"{alias} must be one non-empty URL-safe path segment"
            raise ValueError(message)
        return value

    @field_validator("rest_endpoint")
    @classmethod
    def validate_rest_endpoint(cls, value: str | None) -> str | None:
        """Reject credentials and unstable URL components from task-owned endpoints."""
        if value is None:
            return None
        if value != value.strip() or not _ZEPPELIN_REST_ENDPOINT_PATTERN.fullmatch(
            value
        ):
            message = "restEndpoint must be one literal absolute HTTP(S) endpoint"
            raise ValueError(message)
        try:
            parsed = urlsplit(value)
            parsed_port = parsed.port
        except ValueError as exc:
            message = "restEndpoint must be one valid absolute HTTP(S) endpoint"
            raise ValueError(message) from exc
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or (parsed_port is not None and not 1 <= parsed_port <= 65_535)
        ):
            message = (
                "restEndpoint must be an absolute HTTP(S) endpoint without "
                "credentials, query, or fragment"
            )
            raise ValueError(message)
        return value

    @field_validator("parameters", mode="before")
    @classmethod
    def validate_literal_parameters(cls, value: YamlValue) -> YamlValue:
        """Own only literal string-to-string paragraph parameters."""
        if not isinstance(value, Mapping):
            message = "parameters must be an object of literal string values"
            raise PydanticCustomError(_ZEPPELIN_PARAMETERS_TYPE_ERROR, message)
        normalized: YamlObject = {}
        for raw_key, raw_value in value.items():
            if not isinstance(
                raw_key, str
            ) or not _ZEPPELIN_PARAMETER_KEY_PATTERN.fullmatch(raw_key):
                message = "parameters keys must be portable non-empty names"
                raise ValueError(message)
            if not isinstance(raw_value, str):
                message = "parameters values must be strings"
                raise PydanticCustomError(
                    _ZEPPELIN_PARAMETER_VALUE_TYPE_ERROR,
                    message,
                )
            if (
                _ZEPPELIN_CONTROL_PATTERN.search(raw_value)
                or contains_ds_parameter_placeholder(raw_key)
                or contains_ds_parameter_placeholder(raw_value)
            ):
                message = "parameters must not contain control text or DS placeholders"
                raise ValueError(message)
            normalized[raw_key] = raw_value
        return normalized

    @model_validator(mode="after")
    def validate_connection_fields(self) -> ZeppelinParagraphTaskParamsSpec:
        """Require only fields active for the selected canonical connection mode."""
        active_by_mode = {
            ZeppelinConnectionMode.WORKER_CONFIG: frozenset(),
            ZeppelinConnectionMode.REST_ENDPOINT: frozenset({"rest_endpoint"}),
            ZeppelinConnectionMode.DATASOURCE: frozenset({"datasource"}),
        }
        active_fields = active_by_mode[self.connection_mode]
        for field_name, alias in _ZEPPELIN_CONDITIONAL_FIELD_ALIASES.items():
            value = getattr(self, field_name)
            if field_name in active_fields and value is None:
                message = f"{alias} is required for {self.connection_mode.value}"
                raise ValueError(message)
            if field_name not in active_fields and field_name in self.model_fields_set:
                message = f"{alias} is inactive for {self.connection_mode.value}"
                raise ValueError(message)
        return self
