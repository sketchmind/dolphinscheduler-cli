from __future__ import annotations

from pydantic import Field

from ....common.enums.task_execute_type import TaskExecuteType
from ....plugin.task_api.enums.task_execution_status import TaskExecutionStatus
from ..page_query_dto import PageQueryDto

class TaskInstanceQueryRequest(PageQueryDto):
    """Task instance request"""
    workflowInstanceId: int | None = Field(default=0)
    workflowInstanceName: str | None = Field(default=None)
    workflowDefinitionName: str | None = Field(default=None)
    searchVal: str | None = Field(default=None)
    taskName: str | None = Field(default=None)
    taskCode: int | None = Field(default=None)
    executorName: str | None = Field(default=None)
    stateType: TaskExecutionStatus | None = Field(default=None)
    host: str | None = Field(default=None)
    startTime: str | None = Field(default=None, alias='startDate')
    endTime: str | None = Field(default=None, alias='endDate')
    taskExecuteType: TaskExecuteType | None = Field(default=TaskExecuteType.BATCH)

__all__ = ["TaskInstanceQueryRequest"]
