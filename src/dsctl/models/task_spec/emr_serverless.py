from __future__ import annotations

from pydantic import (
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from dsctl.models.common import GlobalParamSpec
from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)
from dsctl.models.task_spec.emr import (
    validate_emr_json_text,
)
from dsctl.models.task_spec.values import (
    _validate_unique_input_varchar_params,
    contains_ds_parameter_placeholder,
    emr_placeholder_names,
)


class EmrServerlessTaskParamsSpec(TaskParamsSpec):
    """Typed raw StartJobRun authoring for the DS 3.4.2 plugin."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    application_id: str = Field(alias="applicationId")
    execution_role_arn: str = Field(alias="executionRoleArn")
    job_name: str = Field(default="", alias="jobName")
    start_job_run_request_json: str = Field(alias="startJobRunRequestJson")
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )

    @field_validator("application_id")
    @classmethod
    def validate_application_id(cls, value: str) -> str:
        """Require one literal upstream application identity."""
        return _validate_emr_serverless_literal(
            value,
            field="applicationId",
            required=True,
        )

    @field_validator("execution_role_arn")
    @classmethod
    def validate_execution_role_arn(cls, value: str) -> str:
        """Require one literal execution-role identity."""
        return _validate_emr_serverless_literal(
            value,
            field="executionRoleArn",
            required=True,
        )

    @field_validator("job_name")
    @classmethod
    def validate_job_name(cls, value: str) -> str:
        """Allow the empty task-name fallback but not fake substitution."""
        return _validate_emr_serverless_literal(
            value,
            field="jobName",
            required=False,
        )

    @field_validator("start_job_run_request_json")
    @classmethod
    def validate_request_text(cls, value: str) -> str:
        """Require request text while preserving its authored bytes."""
        if not value.strip():
            message = "startJobRunRequestJson must not be empty"
            raise ValueError(message)
        return value

    @model_validator(mode="after")
    def validate_portable_authoring(self) -> EmrServerlessTaskParamsSpec:
        """Validate local bindings and literal JSON before exact projection."""
        _validate_unique_input_varchar_params(
            self.local_params,
            task_type="EMR_SERVERLESS",
        )
        if not emr_placeholder_names(self.start_job_run_request_json):
            validate_emr_json_text(
                self.start_job_run_request_json,
                field="startJobRunRequestJson",
                allow_unresolved_placeholders=False,
            )
        return self


def _validate_emr_serverless_literal(
    value: str,
    *,
    field: str,
    required: bool,
) -> str:
    if required and not value.strip():
        message = f"{field} must not be empty"
        raise ValueError(message)
    if contains_ds_parameter_placeholder(value):
        message = f"{field} does not support DS placeholder substitution"
        raise ValueError(message)
    return value
