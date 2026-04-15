from __future__ import annotations

from pydantic import Field
from ..._models import JsonValue

from ...common.enums.flag import Flag
from ...common.enums.priority import Priority
from ...common.enums.task_execute_type import TaskExecuteType
from ...common.enums.timeout_flag import TimeoutFlag
from ...dao.entities.task_definition import TaskDefinition
from ...dao.entities.workflow_task_relation import WorkflowTaskRelation
from ...plugin.task_api.enums.task_timeout_strategy import TaskTimeoutStrategy
from ...plugin.task_api.model.property import Property

class TaskDefinitionVO(TaskDefinition):
    workflowTaskRelationList: list[WorkflowTaskRelation] | None = Field(default=None)

__all__ = ["TaskDefinitionVO"]
