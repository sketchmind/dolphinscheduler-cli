from __future__ import annotations

import re
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlsplit

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

_MLFLOW_SAFE_TRACKING_URI_PATTERN = re.compile(
    r"[A-Za-z0-9._~:/@%+,\-]+",
)
_MLFLOW_SAFE_MODEL_COMPONENT_PATTERN = re.compile(
    r"[A-Za-z0-9_][A-Za-z0-9._@%+=,\-]*",
)
_MLFLOW_PORT_PATTERN = re.compile(r"[0-9]+")


def _validate_mlflow_tracking_uri(value: str) -> str:
    """Keep the upstream unquoted environment assignment shell-safe."""
    if not value or not _MLFLOW_SAFE_TRACKING_URI_PATTERN.fullmatch(value):
        message = "mlflowTrackingUri must be one shell-safe absolute HTTP endpoint"
        raise ValueError(message)
    try:
        parsed = urlsplit(value)
        parsed_port = parsed.port
    except ValueError as exc:
        message = "mlflowTrackingUri must be one valid absolute HTTP endpoint"
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
            "mlflowTrackingUri must be an absolute HTTP endpoint without "
            "credentials, query, or fragment"
        )
        raise ValueError(message)
    return value


def _validate_mlflow_model_key(value: str) -> str:
    """Accept one conservative MLflow models:/ or runs:/ artifact path."""
    prefix = next(
        (
            candidate
            for candidate in ("models:/", "runs:/")
            if value.startswith(candidate)
        ),
        None,
    )
    if prefix is None:
        message = "deployModelKey must start with models:/ or runs:/"
        raise ValueError(message)
    components = value.removeprefix(prefix).split("/")
    invalid_component_count = (
        len(components) != 2 if prefix == "models:/" else len(components) < 2
    )
    if invalid_component_count or any(
        component in {"", ".", ".."}
        or not _MLFLOW_SAFE_MODEL_COMPONENT_PATTERN.fullmatch(component)
        for component in components
    ):
        message = "deployModelKey must be one portable shell-safe MLflow model path"
        raise ValueError(message)
    return value


class MlflowModelServeTaskParamsSpec(TaskParamsSpec):
    """Shell-safe typed subset for foreground MLflow model serving."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    mlflow_task_type: Literal["MLflow Models"] = Field(alias="mlflowTaskType")
    deploy_type: Literal["MLFLOW"] = Field(alias="deployType")
    mlflow_tracking_uri: str = Field(alias="mlflowTrackingUri")
    deploy_model_key: str = Field(alias="deployModelKey")
    deploy_port: str = Field(alias="deployPort")

    @field_validator("mlflow_tracking_uri")
    @classmethod
    def validate_tracking_uri(cls, value: str) -> str:
        """Reject credentials and shell syntax from the unquoted export value."""
        return _validate_mlflow_tracking_uri(value)

    @field_validator("deploy_model_key")
    @classmethod
    def validate_model_key(cls, value: str) -> str:
        """Reject traversal, placeholders, and shell syntax from the model URI."""
        return _validate_mlflow_model_key(value)

    @field_validator("deploy_port", mode="before")
    @classmethod
    def validate_port(cls, value: YamlValue) -> str:
        """Keep the exact wire string within the TCP port range."""
        if not isinstance(value, str) or not _MLFLOW_PORT_PATTERN.fullmatch(value):
            message = "deployPort must be a decimal string from 1 through 65535"
            raise ValueError(message)
        port = int(value)
        if not 1 <= port <= 65_535:
            message = "deployPort must be a decimal string from 1 through 65535"
            raise ValueError(message)
        return value
