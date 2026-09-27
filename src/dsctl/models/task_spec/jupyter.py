from __future__ import annotations

import re
from collections.abc import Mapping
from typing import TYPE_CHECKING

from pydantic import (
    ConfigDict,
    Field,
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

JUPYTER_CONDA_ENV_NAME_JSON_SCHEMA_PATTERN = (
    r"^(?!.*(?:\.[tT][xX][tT]|\.[tT][aA][rR]\.[gG][zZ])$)"
    r"[A-Za-z0-9_][A-Za-z0-9_.-]*$"
)
JUPYTER_NOTEBOOK_PATH_JSON_SCHEMA_PATTERN = (
    r"^/(?:(?!\.{1,2}/)[A-Za-z0-9_.-]+/)*"
    r"[A-Za-z0-9_][A-Za-z0-9_.-]*\.ipynb$"
)
JUPYTER_PARAMETER_KEY_JSON_SCHEMA_PATTERN = (
    r"^(?!(?:"
    r"[aA][cC][cC][eE][sS][sS]_[kK][eE][yY]|"
    r"[aA][pP][iI]_[kK][eE][yY]|"
    r"[cC][rR][eE][dD][eE][nN][tT][iI][aA][lL]|"
    r"[pP][aA][sS][sS][wW][oO][rR][dD]|"
    r"[pP][aA][sS][sS][wW][dD]|"
    r"[pP][rR][iI][vV][aA][tT][eE]_[kK][eE][yY]|"
    r"[sS][eE][cC][rR][eE][tT]|"
    r"[tT][oO][kK][eE][nN]"
    r")$)[A-Za-z_][A-Za-z0-9_.-]*$"
)
JUPYTER_PARAMETER_VALUE_JSON_SCHEMA_PATTERN = (
    r"^(?![A-Za-z][A-Za-z0-9+.-]*://[^/@]+@)"
    r"[A-Za-z0-9_./][A-Za-z0-9_./:@%+=,-]*$"
)
JUPYTER_OPTION_TOKEN_JSON_SCHEMA_PATTERN = r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$"  # noqa: S105
JUPYTER_SECRET_PARAMETER_NAMES = frozenset(
    {
        "access_key",
        "api_key",
        "credential",
        "password",
        "passwd",
        "private_key",
        "secret",
        "token",
    }
)
_JUPYTER_CONDA_ENV_NAME_PATTERN = re.compile(
    JUPYTER_CONDA_ENV_NAME_JSON_SCHEMA_PATTERN,
)
_JUPYTER_NOTEBOOK_PATH_PATTERN = re.compile(
    JUPYTER_NOTEBOOK_PATH_JSON_SCHEMA_PATTERN,
)
_JUPYTER_PARAMETER_KEY_PATTERN = re.compile(
    JUPYTER_PARAMETER_KEY_JSON_SCHEMA_PATTERN,
)
_JUPYTER_PARAMETER_VALUE_PATTERN = re.compile(
    JUPYTER_PARAMETER_VALUE_JSON_SCHEMA_PATTERN,
)
_JUPYTER_OPTION_TOKEN_PATTERN = re.compile(
    JUPYTER_OPTION_TOKEN_JSON_SCHEMA_PATTERN,
)
_JUPYTER_PARAMETERS_TYPE_ERROR = "jupyter_parameters_type"


class JupyterNotebookTaskParamsSpec(TaskParamsSpec):
    """Shell-safe typed subset for one pre-installed Jupyter notebook run."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    conda_env_name: str = Field(alias="condaEnvName")
    input_note_path: str = Field(alias="inputNotePath")
    output_note_path: str = Field(alias="outputNotePath")
    parameters: dict[str, str] = Field(
        default_factory=dict,
        json_schema_extra={"default": {}},
    )
    kernel: str | None = None
    engine: str | None = None
    execution_timeout: int | None = Field(
        default=None,
        alias="executionTimeout",
        gt=0,
    )
    start_timeout: int | None = Field(
        default=None,
        alias="startTimeout",
        gt=0,
    )

    def default_payload_field_names(self) -> tuple[str, ...]:
        """Keep the canonical empty parameter map explicit until projection."""
        return ("parameters",)

    @field_validator("conda_env_name")
    @classmethod
    def validate_preinstalled_conda_env_name(cls, value: str) -> str:
        """Own only a pre-installed environment, never resource-backed setup."""
        if not _JUPYTER_CONDA_ENV_NAME_PATTERN.fullmatch(value):
            message = "condaEnvName must name one shell-safe pre-installed environment"
            raise ValueError(message)
        return value

    @field_validator("input_note_path", "output_note_path")
    @classmethod
    def validate_notebook_path(cls, value: str) -> str:
        """Require one literal absolute POSIX notebook path."""
        if not _JUPYTER_NOTEBOOK_PATH_PATTERN.fullmatch(value):
            message = "notebook paths must be safe absolute POSIX .ipynb paths"
            raise ValueError(message)
        if any(component in {".", ".."} for component in value.split("/")):
            message = "notebook paths must not contain dot traversal components"
            raise ValueError(message)
        return value

    @field_validator("parameters", mode="before")
    @classmethod
    def validate_parameters(cls, value: YamlValue) -> YamlValue:
        """Reject shell expansion and the bounded obvious-secret denylist."""
        if not isinstance(value, Mapping):
            message = "parameters must be an object of shell-safe string tokens"
            raise PydanticCustomError(_JUPYTER_PARAMETERS_TYPE_ERROR, message)
        normalized: YamlObject = {}
        for raw_key, raw_value in value.items():
            if (
                not isinstance(raw_key, str)
                or not _JUPYTER_PARAMETER_KEY_PATTERN.fullmatch(raw_key)
                or raw_key.lower() in JUPYTER_SECRET_PARAMETER_NAMES
            ):
                message = (
                    "parameters keys must be safe and outside the secret-name denylist"
                )
                raise ValueError(message)
            if not isinstance(
                raw_value, str
            ) or not _JUPYTER_PARAMETER_VALUE_PATTERN.fullmatch(raw_value):
                message = "parameters values must be safe tokens without URI userinfo"
                raise ValueError(message)
            normalized[raw_key] = raw_value
        return normalized

    @field_validator("kernel", "engine")
    @classmethod
    def validate_optional_option_token(cls, value: str | None) -> str | None:
        """Keep optional papermill selectors to one literal shell word."""
        if value is not None and not _JUPYTER_OPTION_TOKEN_PATTERN.fullmatch(value):
            message = "kernel and engine must be shell-safe option tokens"
            raise ValueError(message)
        return value

    @field_validator("execution_timeout", "start_timeout", mode="before")
    @classmethod
    def validate_strict_positive_timeout(cls, value: YamlValue) -> YamlValue:
        """Reject bool/string coercion before enforcing positive seconds."""
        if value is not None and (
            not isinstance(value, int) or isinstance(value, bool)
        ):
            message = "JUPYTER timeouts must be strict positive integers"
            raise ValueError(message)
        return value

    @model_validator(mode="after")
    def validate_distinct_notebook_paths(
        self,
    ) -> JupyterNotebookTaskParamsSpec:
        """Prevent papermill input/output aliasing in the portable subset."""
        if self.input_note_path == self.output_note_path:
            message = "inputNotePath and outputNotePath must be different"
            raise ValueError(message)
        return self
