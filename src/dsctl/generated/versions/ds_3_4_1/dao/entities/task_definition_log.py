from __future__ import annotations

from pydantic import Field
from ..._models import JsonValue

from ...common.enums.flag import Flag
from ...common.enums.priority import Priority
from ...common.enums.task_execute_type import TaskExecuteType
from ...common.enums.timeout_flag import TimeoutFlag
from ...plugin.task_api.enums.task_timeout_strategy import TaskTimeoutStrategy
from ...plugin.task_api.model.property import Property
from .task_definition import TaskDefinition

class TaskDefinitionLog(TaskDefinition):
    """Task definition log"""
    operator: int = Field(default=0, description='operator user id')
    operateTime: str | None = Field(default=None, description='operate time')

__all__ = ["TaskDefinitionLog"]
