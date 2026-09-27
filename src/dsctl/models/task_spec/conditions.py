from __future__ import annotations

from typing import Literal

from pydantic import (
    ConfigDict,
    Field,
    field_validator,
)

from dsctl.models.common import (
    GlobalParamSpec,
    YamlSpecModel,
)
from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)
from dsctl.models.task_spec.dependent import DependentRelation
from dsctl.models.task_spec.values import (
    _normalize_task_name_ref,
)


class ConditionPredicateSpec(YamlSpecModel):
    """One local task predicate evaluated by a CONDITIONS task."""

    task: str
    status: Literal["SUCCESS", "FAILURE"]

    @field_validator("task")
    @classmethod
    def validate_task_name(cls, value: str) -> str:
        """Normalize the referenced local task name."""
        return _normalize_task_name_ref(value)


class ConditionDependentTaskModelSpec(YamlSpecModel):
    """One CONDITIONS upstream branch group."""

    model_config = ConfigDict(populate_by_name=True)

    depend_item_list: list[ConditionPredicateSpec] = Field(alias="dependItemList")
    relation: DependentRelation

    @field_validator("depend_item_list")
    @classmethod
    def validate_non_empty_items(
        cls,
        value: list[ConditionPredicateSpec],
    ) -> list[ConditionPredicateSpec]:
        """Require at least one upstream item per CONDITIONS branch."""
        if not value:
            message = "dependItemList must not be empty"
            raise ValueError(message)
        return value


class ConditionDependencySpec(YamlSpecModel):
    """Upstream dependency tree for one CONDITIONS task."""

    model_config = ConfigDict(populate_by_name=True)

    depend_task_list: list[ConditionDependentTaskModelSpec] = Field(
        alias="dependTaskList"
    )
    relation: DependentRelation

    @field_validator("depend_task_list")
    @classmethod
    def validate_non_empty_tasks(
        cls,
        value: list[ConditionDependentTaskModelSpec],
    ) -> list[ConditionDependentTaskModelSpec]:
        """Require at least one CONDITIONS upstream branch."""
        if not value:
            message = "dependTaskList must not be empty"
            raise ValueError(message)
        return value


class ConditionResultSpec(YamlSpecModel):
    """Downstream branch selection for one CONDITIONS task."""

    model_config = ConfigDict(populate_by_name=True)

    success_node: list[str] = Field(alias="successNode")
    failed_node: list[str] = Field(alias="failedNode")

    @field_validator("success_node", "failed_node")
    @classmethod
    def validate_non_empty_nodes(cls, value: list[str]) -> list[str]:
        """Require at least one branch target per CONDITIONS outcome."""
        if not value:
            message = "Branch target lists must not be empty"
            raise ValueError(message)
        return [_normalize_task_name_ref(item) for item in value]


class ConditionsTaskParamsSpec(TaskParamsSpec):
    """Typed YAML shape for CONDITIONS task params."""

    dependence: ConditionDependencySpec
    condition_result: ConditionResultSpec = Field(alias="conditionResult")
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )
    var_pool: list[GlobalParamSpec] = Field(default_factory=list, alias="varPool")
