from __future__ import annotations

import re
from enum import StrEnum

from pydantic import (
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)


class DvcTaskType(StrEnum):
    """Exact native DVC operation selected by one task definition."""

    UPLOAD = "Upload"
    DOWNLOAD = "Download"
    INIT = "Init DVC"


_DVC_SAFE_SHELL_TOKEN_PATTERN = re.compile(
    r"[A-Za-z0-9_./~][A-Za-z0-9_./~:@%+=,\-]*",
)
_DVC_SAFE_REF_PATTERN = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._/\-]*")
_DVC_URI_USERINFO_PATTERN = re.compile(
    r"^[A-Za-z][A-Za-z0-9+.-]*://[^/@]+@",
)
_DVC_UNSAFE_MESSAGE_PATTERN = re.compile(r'[\x00-\x1f\x7f"\\$`]')
_DVC_CONDITIONAL_FIELD_ALIASES = {
    "dvc_data_location": "dvcDataLocation",
    "dvc_load_save_data_path": "dvcLoadSaveDataPath",
    "dvc_version": "dvcVersion",
    "dvc_message": "dvcMessage",
    "dvc_store_url": "dvcStoreUrl",
}


def _validate_dvc_shell_token(
    value: str,
    *,
    field: str,
    reject_uri_userinfo: bool,
) -> str:
    """Keep values safe in the upstream plugin's unquoted POSIX shell slots."""
    if not _DVC_SAFE_SHELL_TOKEN_PATTERN.fullmatch(value):
        message = f"{field} must be one safe POSIX shell token"
        raise ValueError(message)
    if reject_uri_userinfo and _DVC_URI_USERINFO_PATTERN.search(value):
        message = f"{field} must not contain URI userinfo credentials"
        raise ValueError(message)
    return value


def _validate_dvc_ref(value: str) -> str:
    """Keep upload tags and download revisions in a portable Git-ref subset."""
    components = value.split("/")
    if (
        not _DVC_SAFE_REF_PATTERN.fullmatch(value)
        or ".." in value
        or "//" in value
        or value.endswith((".", "/"))
        or any(part.startswith(".") or part.endswith(".lock") for part in components)
    ):
        message = "dvcVersion must be one portable Git tag, branch, or SHA"
        raise ValueError(message)
    return value


class DvcTaskParamsSpec(TaskParamsSpec):
    """Shell-safe typed subset for one native DVC operation."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    dvc_task_type: DvcTaskType = Field(alias="dvcTaskType")
    dvc_repository: str = Field(alias="dvcRepository")
    dvc_version: str | None = Field(default=None, alias="dvcVersion")
    dvc_data_location: str | None = Field(default=None, alias="dvcDataLocation")
    dvc_message: str | None = Field(default=None, alias="dvcMessage")
    dvc_load_save_data_path: str | None = Field(
        default=None,
        alias="dvcLoadSaveDataPath",
    )
    dvc_store_url: str | None = Field(default=None, alias="dvcStoreUrl")

    @field_validator("dvc_repository")
    @classmethod
    def validate_repository(cls, value: str) -> str:
        """Reject command options, expansion, and logged inline credentials."""
        return _validate_dvc_shell_token(
            value,
            field="dvcRepository",
            reject_uri_userinfo=True,
        )

    @field_validator(
        "dvc_version",
        "dvc_data_location",
        "dvc_load_save_data_path",
    )
    @classmethod
    def validate_optional_shell_token(
        cls,
        value: str | None,
        info: ValidationInfo,
    ) -> str | None:
        """Validate optional values used as unquoted shell words."""
        if value is None:
            return None
        field_name = info.field_name
        if field_name is None:
            message = "DVC shell field identity is missing"
            raise TypeError(message)
        alias = _DVC_CONDITIONAL_FIELD_ALIASES[field_name]
        normalized = _validate_dvc_shell_token(
            value,
            field=alias,
            reject_uri_userinfo=True,
        )
        return (
            _validate_dvc_ref(normalized) if field_name == "dvc_version" else normalized
        )

    @field_validator("dvc_store_url")
    @classmethod
    def validate_store_url(cls, value: str | None) -> str | None:
        """Reject unsafe store tokens and credentials that upstream logs."""
        if value is None:
            return None
        return _validate_dvc_shell_token(
            value,
            field="dvcStoreUrl",
            reject_uri_userinfo=True,
        )

    @field_validator("dvc_message")
    @classmethod
    def validate_message(cls, value: str | None) -> str | None:
        """Keep the value safe inside the upstream double-quoted assignment."""
        if value is None:
            return None
        if not value.strip() or _DVC_UNSAFE_MESSAGE_PATTERN.search(value):
            message = "dvcMessage contains unsafe shell expansion or control text"
            raise ValueError(message)
        return value

    @model_validator(mode="after")
    def validate_mode_fields(self) -> DvcTaskParamsSpec:
        """Require exactly the fields consumed by the selected native mode."""
        active_by_mode = {
            DvcTaskType.UPLOAD: frozenset(
                {
                    "dvc_data_location",
                    "dvc_load_save_data_path",
                    "dvc_version",
                    "dvc_message",
                }
            ),
            DvcTaskType.DOWNLOAD: frozenset(
                {
                    "dvc_data_location",
                    "dvc_load_save_data_path",
                    "dvc_version",
                }
            ),
            DvcTaskType.INIT: frozenset({"dvc_store_url"}),
        }
        active_fields = active_by_mode[self.dvc_task_type]
        for field_name, alias in _DVC_CONDITIONAL_FIELD_ALIASES.items():
            value = getattr(self, field_name)
            if field_name in active_fields and value is None:
                message = f"{alias} is required for {self.dvc_task_type.value}"
                raise ValueError(message)
            if field_name not in active_fields and field_name in self.model_fields_set:
                message = f"{alias} is inactive for {self.dvc_task_type.value}"
                raise ValueError(message)
        return self
