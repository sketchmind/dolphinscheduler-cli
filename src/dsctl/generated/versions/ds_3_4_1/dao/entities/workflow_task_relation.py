from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel, JsonValue

from ...common.enums.condition_type import ConditionType

class WorkflowTaskRelation(BaseEntityModel):
    id: int | None = Field(default=None)
    name: str | None = Field(default=None)
    workflowDefinitionVersion: int = Field(default=0)
    projectCode: int = Field(default=0)
    workflowDefinitionCode: int = Field(default=0)
    preTaskCode: int = Field(default=0)
    preTaskVersion: int = Field(default=0)
    postTaskCode: int = Field(default=0)
    postTaskVersion: int = Field(default=0)
    conditionType: ConditionType | None = Field(default=None)
    conditionParams: JsonValue | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["WorkflowTaskRelation"]
