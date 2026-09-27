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
from dsctl.models.task_spec.datasource_ref import DatasourceReference
from dsctl.models.task_spec.values import (
    contains_ds_parameter_placeholder,
    validate_json_object_text,
)


class SagemakerStartPipelineExecutionTaskParamsSpec(TaskParamsSpec):
    """Canonical intent for one SageMaker StartPipelineExecution request."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, strict=True)

    sagemaker_request_json: str = Field(
        alias="sagemakerRequestJson",
        min_length=1,
        description=(
            "Preserved StartPipelineExecution request text. DolphinScheduler "
            "substitutes task parameters before parsing the request as JSON and "
            "logs the resolved request; this field is not secret storage."
        ),
    )
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )
    datasource: DatasourceReference | None = Field(
        default=None,
        description=(
            "Positive DolphinScheduler SageMaker datasource id or exact name on "
            "profiles whose worker resolves datasource-backed credentials."
        ),
    )

    @field_validator("sagemaker_request_json")
    @classmethod
    def validate_request_text(cls, value: str) -> str:
        """Reject blank requests while preserving every authored character."""
        if not value.strip():
            message = "sagemakerRequestJson must not be blank"
            raise ValueError(message)
        return value

    @model_validator(mode="after")
    def validate_request_and_local_params(
        self,
    ) -> SagemakerStartPipelineExecutionTaskParamsSpec:
        """Validate literal JSON now and leave placeholder templates for runtime."""
        seen_names: set[str] = set()
        duplicate_names: set[str] = set()
        for parameter in self.local_params:
            if parameter.prop in seen_names:
                duplicate_names.add(parameter.prop)
            seen_names.add(parameter.prop)
        if duplicate_names:
            names = ", ".join(sorted(duplicate_names))
            message = (
                f"SAGEMAKER localParams prop names must be unique; duplicates: {names}"
            )
            raise ValueError(message)
        if not contains_ds_parameter_placeholder(self.sagemaker_request_json):
            validate_json_object_text(
                self.sagemaker_request_json,
                field="sagemakerRequestJson",
            )
        return self
