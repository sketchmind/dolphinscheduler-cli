from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

from .task_definition import TaskDefinition
from .workflow_definition import WorkflowDefinition
from .workflow_task_relation import WorkflowTaskRelation

class DagData(BaseEntityModel):
    workflowDefinition: WorkflowDefinition | None = Field(default=None)
    workflowTaskRelationList: list[WorkflowTaskRelation] | None = Field(default=None)
    taskDefinitionList: list[TaskDefinition] | None = Field(default=None)

__all__ = ["DagData"]
