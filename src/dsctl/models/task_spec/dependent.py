from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import (
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from dsctl.models.common import (
    GlobalParamSpec,
    YamlObject,
    YamlSpecModel,
)
from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)
from dsctl.models.task_spec.values import (
    contains_ds_parameter_placeholder,
)


class DependentRelation(StrEnum):
    """Supported DS dependent relation values."""

    AND = "AND"
    OR = "OR"


class DependentType(StrEnum):
    """Supported DS dependent item target kinds."""

    DEPENDENT_ON_WORKFLOW = "DEPENDENT_ON_WORKFLOW"
    DEPENDENT_ON_TASK = "DEPENDENT_ON_TASK"


DEPENDENT_HOUR_DATE_VALUES = (
    "currentHour",
    "last1Hour",
    "last2Hours",
    "last3Hours",
    "last24Hours",
)
DEPENDENT_DAY_DATE_VALUES = (
    "today",
    "last1Days",
    "last2Days",
    "last3Days",
    "last7Days",
)
DEPENDENT_WEEK_DATE_VALUES = (
    "thisWeek",
    "lastWeek",
    "lastMonday",
    "lastTuesday",
    "lastWednesday",
    "lastThursday",
    "lastFriday",
    "lastSaturday",
    "lastSunday",
)
DEPENDENT_BASE_MONTH_DATE_VALUES = (
    "thisMonth",
    "lastMonth",
    "lastMonthBegin",
    "lastMonthEnd",
)
DEPENDENT_EXTENDED_MONTH_DATE_VALUES = (
    "thisMonth",
    "thisMonthBegin",
    "thisMonthEnd",
    "lastMonth",
    "lastMonthBegin",
    "lastMonthEnd",
)


class DependResult(StrEnum):
    """Supported DS dependent result states."""

    SUCCESS = "SUCCESS"
    WAITING = "WAITING"
    FAILED = "FAILED"
    NON_EXEC = "NON_EXEC"


class DependentFailurePolicy(StrEnum):
    """Supported DS dependent failure policies."""

    DEPENDENT_FAILURE_FAILURE = "DEPENDENT_FAILURE_FAILURE"
    DEPENDENT_FAILURE_WAITING = "DEPENDENT_FAILURE_WAITING"


class DependentItemSpec(YamlSpecModel):
    """One DS dependent item inside one dependent task branch."""

    model_config = ConfigDict(populate_by_name=True)

    dependent_type: DependentType = Field(alias="dependentType")
    project_code: int = Field(alias="projectCode", ge=1)
    definition_code: int = Field(alias="definitionCode", ge=1)
    dep_task_code: int = Field(alias="depTaskCode", ge=0)
    cycle: Literal["hour", "day", "week", "month"]
    date_value: str = Field(alias="dateValue")
    parameter_passing: bool | None = Field(default=None, alias="parameterPassing")

    @field_validator("cycle", "date_value")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Reject blank dependent item text fields after trimming."""
        normalized = value.strip()
        if not normalized:
            message = "Dependent item text fields must not be empty"
            raise ValueError(message)
        return normalized

    @model_validator(mode="after")
    def validate_target_and_date_window(self) -> DependentItemSpec:
        """Reject contradictory selectors and runtime-unknown date windows."""
        expected_type = (
            DependentType.DEPENDENT_ON_WORKFLOW
            if self.dep_task_code == 0
            else DependentType.DEPENDENT_ON_TASK
        )
        if self.dependent_type is not expected_type:
            message = (
                f"dependentType {self.dependent_type.value} is inconsistent with "
                f"depTaskCode {self.dep_task_code}"
            )
            raise ValueError(message)
        _validate_dependent_date_window(
            self.cycle,
            self.date_value,
            month_values=DEPENDENT_EXTENDED_MONTH_DATE_VALUES,
        )
        return self


def _validate_dependent_date_window(
    cycle: str,
    date_value: str,
    *,
    month_values: tuple[str, ...],
) -> None:
    """Require the dateValue branch actually implemented for one cycle."""
    values_by_cycle = {
        "hour": DEPENDENT_HOUR_DATE_VALUES,
        "day": DEPENDENT_DAY_DATE_VALUES,
        "week": DEPENDENT_WEEK_DATE_VALUES,
        "month": month_values,
    }
    allowed = values_by_cycle.get(cycle, ())
    if date_value not in allowed:
        choices = ", ".join(allowed)
        message = (
            f"dateValue {date_value!r} is invalid for cycle {cycle!r}; "
            f"expected one of: {choices}"
        )
        raise ValueError(message)


def _normalize_dependent_139_name(
    value: str,
    *,
    field: str,
    allow_all: bool = True,
) -> str:
    """Normalize one literal name used by the exact legacy resolver."""
    normalized = value.strip()
    if not normalized:
        message = f"{field} must not be blank"
        raise ValueError(message)
    if contains_ds_parameter_placeholder(normalized):
        message = f"{field} must be a literal name without DS placeholders"
        raise ValueError(message)
    if not allow_all and normalized == "ALL":
        message = f"{field} must not use the reserved DEPENDENT sentinel ALL"
        raise ValueError(message)
    return normalized


class Dependent139ItemSpec(YamlSpecModel):
    """Canonical name-routed item for the split DolphinScheduler 1.3.9 wire."""

    model_config = ConfigDict(populate_by_name=True)

    dependent_type: DependentType = Field(alias="dependentType")
    project_name: str = Field(alias="projectName")
    workflow_name: str = Field(alias="workflowName")
    task_name: str | None = Field(default=None, alias="taskName")
    cycle: Literal["hour", "day", "week", "month"]
    date_value: str = Field(alias="dateValue")

    @field_validator("project_name", "workflow_name")
    @classmethod
    def validate_workflow_identity(cls, value: str, info: ValidationInfo) -> str:
        """Keep project and workflow selectors literal and unambiguous."""
        field_name = info.field_name
        if field_name is None:
            message = "DEPENDENT identity validator requires a named field"
            raise ValueError(message)
        alias = cls.model_fields[field_name].alias or field_name
        return _normalize_dependent_139_name(value, field=alias)

    @field_validator("task_name")
    @classmethod
    def validate_task_identity(cls, value: str | None) -> str | None:
        """Keep task selectors literal and distinct from the native ALL sentinel."""
        if value is None:
            return None
        return _normalize_dependent_139_name(
            value,
            field="taskName",
            allow_all=False,
        )

    @field_validator("date_value")
    @classmethod
    def validate_date_value_text(cls, value: str) -> str:
        """Reject blank values before applying the exact runtime table."""
        normalized = value.strip()
        if not normalized:
            message = "dateValue must not be blank"
            raise ValueError(message)
        return normalized

    @model_validator(mode="after")
    def validate_target_and_date_window(self) -> Dependent139ItemSpec:
        """Bind target kind to taskName and the exact base-23 date table."""
        if self.dependent_type is DependentType.DEPENDENT_ON_WORKFLOW:
            if self.task_name is not None:
                message = "taskName must be omitted for DEPENDENT_ON_WORKFLOW"
                raise ValueError(message)
        elif self.task_name is None:
            message = "taskName is required for DEPENDENT_ON_TASK"
            raise ValueError(message)
        _validate_dependent_date_window(
            self.cycle,
            self.date_value,
            month_values=DEPENDENT_BASE_MONTH_DATE_VALUES,
        )
        return self


class Dependent139TaskModelSpec(YamlSpecModel):
    """One nonempty exact 1.3.9 dependent branch group."""

    model_config = ConfigDict(populate_by_name=True)

    depend_item_list: list[Dependent139ItemSpec] = Field(alias="dependItemList")
    relation: DependentRelation

    @field_validator("depend_item_list")
    @classmethod
    def validate_non_empty_items(
        cls,
        value: list[Dependent139ItemSpec],
    ) -> list[Dependent139ItemSpec]:
        """Avoid the upstream executor's vacuous empty-group success."""
        if not value:
            message = "dependItemList must not be empty"
            raise ValueError(message)
        return value


class Dependent139DependenceSpec(YamlSpecModel):
    """Exact 1.3.9 dependence tree before compiler-owned ID projection."""

    model_config = ConfigDict(populate_by_name=True)

    depend_task_list: list[Dependent139TaskModelSpec] = Field(alias="dependTaskList")
    relation: DependentRelation

    @field_validator("depend_task_list")
    @classmethod
    def validate_non_empty_tasks(
        cls,
        value: list[Dependent139TaskModelSpec],
    ) -> list[Dependent139TaskModelSpec]:
        """Avoid the upstream executor's vacuous empty-tree success."""
        if not value:
            message = "dependTaskList must not be empty"
            raise ValueError(message)
        return value


class Dependent139TaskParamsSpec(TaskParamsSpec):
    """Closed canonical params for split-wire DolphinScheduler 1.3.9 DEPENDENT."""

    dependence: Dependent139DependenceSpec


class DependentTaskModelSpec(YamlSpecModel):
    """One dependent task branch group."""

    model_config = ConfigDict(populate_by_name=True)

    depend_item_list: list[DependentItemSpec] = Field(alias="dependItemList")
    relation: DependentRelation

    @field_validator("depend_item_list")
    @classmethod
    def validate_non_empty_items(
        cls,
        value: list[DependentItemSpec],
    ) -> list[DependentItemSpec]:
        """Require at least one dependent item per branch."""
        if not value:
            message = "dependItemList must not be empty"
            raise ValueError(message)
        return value


class DependenceSpec(YamlSpecModel):
    """Dependent task dependence tree."""

    model_config = ConfigDict(populate_by_name=True)

    depend_task_list: list[DependentTaskModelSpec] = Field(alias="dependTaskList")
    relation: DependentRelation
    check_interval: int | None = Field(default=None, alias="checkInterval", gt=0)
    failure_policy: DependentFailurePolicy | None = Field(
        default=None,
        alias="failurePolicy",
    )
    failure_waiting_time: int | None = Field(
        default=None,
        alias="failureWaitingTime",
        ge=0,
    )

    @field_validator("depend_task_list")
    @classmethod
    def validate_non_empty_tasks(
        cls,
        value: list[DependentTaskModelSpec],
    ) -> list[DependentTaskModelSpec]:
        """Require at least one dependent task branch."""
        if not value:
            message = "dependTaskList must not be empty"
            raise ValueError(message)
        return value


class DependentTaskParamsSpec(TaskParamsSpec):
    """Typed YAML shape for DEPENDENT task params."""

    dependence: DependenceSpec
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )
    resource_list: list[YamlObject] = Field(
        default_factory=list,
        alias="resourceList",
    )
    var_pool: list[GlobalParamSpec] = Field(default_factory=list, alias="varPool")
