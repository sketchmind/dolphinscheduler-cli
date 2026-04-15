from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field

from ...common.enums.flag import Flag
from ...common.enums.priority import Priority
from ...common.enums.task_execute_type import TaskExecuteType
from ...plugin.task_api.enums.task_execution_status import TaskExecutionStatus
from .abstract_task_instance_context import AbstractTaskInstanceContext
from .task_definition import TaskDefinition
from .task_instance import TaskInstance
from .workflow_definition import WorkflowDefinition
from .workflow_instance import WorkflowInstance

T = TypeVar("T")

class TaskInstanceDependentDetails(TaskInstance, Generic[T]):
    taskInstanceDependentResults: list[T] | None = Field(default=None)

class TaskInstanceDependentDetailsAbstractTaskInstanceContext(TaskInstanceDependentDetails[AbstractTaskInstanceContext]):
    """Specialized view for TaskInstanceDependentDetails<AbstractTaskInstanceContext>."""

__all__ = ["TaskInstanceDependentDetails", "TaskInstanceDependentDetailsAbstractTaskInstanceContext"]
