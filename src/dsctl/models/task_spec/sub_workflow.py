from __future__ import annotations

from pydantic import (
    Field,
    field_validator,
    model_validator,
)

from dsctl.models.common import GlobalParamSpec, YamlObject
from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)
from dsctl.models.task_spec.values import (
    contains_ds_parameter_placeholder,
)


class SubWorkflowTaskParamsSpec(TaskParamsSpec):
    """Typed YAML shape for SUB_WORKFLOW task params."""

    workflow_definition_code: int | None = Field(
        default=None,
        alias="workflowDefinitionCode",
        ge=1,
    )
    child_workflow_name: str | None = Field(default=None, alias="childWorkflowName")
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )
    resource_list: list[YamlObject] = Field(
        default_factory=list,
        alias="resourceList",
    )
    var_pool: list[GlobalParamSpec] = Field(default_factory=list, alias="varPool")

    @field_validator("child_workflow_name")
    @classmethod
    def validate_child_workflow(cls, value: str | None) -> str | None:
        """Normalize one human-facing child workflow selector."""
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            message = "childWorkflowName must not be blank"
            raise ValueError(message)
        if contains_ds_parameter_placeholder(normalized):
            message = "childWorkflowName must be a literal name without DS placeholders"
            raise ValueError(message)
        return normalized

    @model_validator(mode="after")
    def validate_exactly_one_workflow_selector(self) -> SubWorkflowTaskParamsSpec:
        """Keep native codes and human-facing selectors mutually exclusive."""
        if (self.workflow_definition_code is None) == (
            self.child_workflow_name is None
        ):
            message = (
                "exactly one of childWorkflowName or workflowDefinitionCode is required"
            )
            raise ValueError(message)
        return self
