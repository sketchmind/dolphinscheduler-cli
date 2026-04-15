from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

class DependentLineageTask(BaseEntityModel):
    projectCode: int = Field(default=0)
    workflowDefinitionCode: int = Field(default=0)
    workflowDefinitionName: str | None = Field(default=None)
    taskDefinitionCode: int = Field(default=0)
    taskDefinitionName: str | None = Field(default=None)

__all__ = ["DependentLineageTask"]
