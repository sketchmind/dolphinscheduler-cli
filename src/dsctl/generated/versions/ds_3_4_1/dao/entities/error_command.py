from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

from ...common.enums.command_type import CommandType
from ...common.enums.failure_strategy import FailureStrategy
from ...common.enums.priority import Priority
from ...common.enums.task_depend_type import TaskDependType
from ...common.enums.warning_type import WarningType

class ErrorCommand(BaseEntityModel):
    id: int | None = Field(default=None)
    commandType: CommandType | None = Field(default=None)
    workflowDefinitionCode: int = Field(default=0)
    workflowDefinitionVersion: int = Field(default=0)
    workflowInstanceId: int = Field(default=0)
    executorId: int = Field(default=0)
    commandParam: str | None = Field(default=None)
    taskDependType: TaskDependType | None = Field(default=None)
    failureStrategy: FailureStrategy | None = Field(default=None)
    warningType: WarningType | None = Field(default=None)
    warningGroupId: int | None = Field(default=None)
    scheduleTime: str | None = Field(default=None)
    startTime: str | None = Field(default=None)
    workflowInstancePriority: Priority | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    message: str | None = Field(default=None)
    workerGroup: str | None = Field(default=None)
    tenantCode: str | None = Field(default=None)
    environmentCode: int | None = Field(default=None)
    dryRun: int = Field(default=0)

__all__ = ["ErrorCommand"]
