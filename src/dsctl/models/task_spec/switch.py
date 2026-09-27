from __future__ import annotations

from pydantic import (
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from dsctl.models.common import (
    GlobalParamSpec,
    YamlSpecModel,
)
from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)
from dsctl.models.task_spec.values import (
    _normalize_task_name_ref,
)


class SwitchBranchSpec(YamlSpecModel):
    """One conditional branch in one SWITCH task."""

    model_config = ConfigDict(populate_by_name=True)

    condition: str
    next_node: str = Field(alias="nextNode")

    @field_validator("condition", "next_node")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Reject blank branch expressions and branch targets."""
        return _normalize_task_name_ref(value)


class SwitchResultSpec(YamlSpecModel):
    """Branching payload used by SWITCH task params."""

    model_config = ConfigDict(populate_by_name=True)

    depend_task_list: list[SwitchBranchSpec] = Field(
        default_factory=list,
        alias="dependTaskList",
    )
    next_node: str | None = Field(default=None, alias="nextNode")

    @field_validator("next_node")
    @classmethod
    def validate_optional_next_node(cls, value: str | None) -> str | None:
        """Normalize the default branch target when present."""
        if value is None:
            return None
        return _normalize_task_name_ref(value)

    @model_validator(mode="after")
    def validate_branch_targets(self) -> SwitchResultSpec:
        """Require at least one branch target or one default branch target."""
        if not self.depend_task_list and self.next_node is None:
            message = "switchResult must define dependTaskList or nextNode"
            raise ValueError(message)
        return self


class SwitchTaskParamsSpec(TaskParamsSpec):
    """Typed YAML shape for SWITCH task params."""

    switch_result: SwitchResultSpec = Field(alias="switchResult")
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )
    var_pool: list[GlobalParamSpec] = Field(default_factory=list, alias="varPool")

    @model_validator(mode="after")
    def reject_runtime_next_branch(self) -> SwitchTaskParamsSpec:
        """Keep DS runtime branch evidence outside typed authoring."""
        if self.model_extra is not None and "nextBranch" in self.model_extra:
            message = "nextBranch is runtime state and cannot be authored"
            raise ValueError(message)
        return self
