from __future__ import annotations

from typing import Literal

from pydantic import (
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from dsctl.models.common import (
    YamlSpecModel,
)
from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)
from dsctl.models.task_spec.values import (
    _normalize_task_name_ref,
    contains_ds_parameter_placeholder,
)


class BlockingPredicateSpec(YamlSpecModel):
    """One same-workflow task-state predicate evaluated by BLOCKING."""

    model_config = ConfigDict(extra="forbid", strict=True)

    task: str
    status: Literal["SUCCESS", "FAILURE"]

    @field_validator("task")
    @classmethod
    def validate_task_name(cls, value: str) -> str:
        """Normalize the referenced same-workflow task name."""
        return _normalize_task_name_ref(value)


class BlockingDependentTaskModelSpec(YamlSpecModel):
    """One nonempty predicate group in a BLOCKING dependency tree."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=False,
        validate_by_alias=True,
        validate_by_name=False,
        strict=True,
    )

    depend_item_list: list[BlockingPredicateSpec] = Field(
        alias="dependItemList",
        min_length=1,
    )
    relation: Literal["AND", "OR"]


class BlockingDependencySpec(YamlSpecModel):
    """Closed same-workflow dependency tree for one BLOCKING task."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=False,
        validate_by_alias=True,
        validate_by_name=False,
        strict=True,
    )

    depend_task_list: list[BlockingDependentTaskModelSpec] = Field(
        alias="dependTaskList",
        min_length=1,
    )
    relation: Literal["AND", "OR"]


class BlockingTaskParamsSpec(TaskParamsSpec):
    """Closed same-workflow state gate for a BLOCKING task."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=False,
        validate_by_alias=True,
        validate_by_name=False,
        strict=True,
        json_schema_extra={
            "x-dsctl-runtime-validations": [
                "dependTaskList and every dependItemList must be nonempty",
                "predicate task names must resolve inside the same workflow",
                "predicate task names are compiled to positive task codes",
                "predicate task references become real DAG predecessors",
            ]
        },
    )

    blocking_opportunity: Literal["BlockingOnSuccess", "BlockingOnFailed"] = Field(
        alias="blockingOpportunity",
        description=(
            "Aggregate dependency state that moves the workflow to terminal BLOCK."
        ),
    )
    alert_when_blocking: bool = Field(
        default=False,
        alias="alertWhenBlocking",
        description=(
            "Request a blocking alert record for the workflow warningGroupId after "
            "the gate matches; delivery requires separately configured alert "
            "infrastructure."
        ),
    )
    dependence: BlockingDependencySpec

    @model_validator(mode="after")
    def reject_placeholder_task_references(self) -> BlockingTaskParamsSpec:
        """Keep dependency identity compile-time literal and same-workflow bound."""
        for group in self.dependence.depend_task_list:
            for predicate in group.depend_item_list:
                if contains_ds_parameter_placeholder(predicate.task):
                    message = (
                        "BLOCKING predicate task names must not contain "
                        "DolphinScheduler placeholders"
                    )
                    raise ValueError(message)
        return self

    def default_payload_field_names(self) -> tuple[str, ...]:
        """Always materialize the exact native false alert default."""
        return ("alert_when_blocking",)
