from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

from ...common.enums.task_group_queue_status import TaskGroupQueueStatus

class TaskGroupQueue(BaseEntityModel):
    id: int | None = Field(default=None)
    taskId: int = Field(default=0)
    taskName: str | None = Field(default=None)
    projectName: str | None = Field(default=None)
    projectCode: str | None = Field(default=None)
    workflowInstanceName: str | None = Field(default=None)
    groupId: int = Field(default=0)
    workflowInstanceId: int | None = Field(default=None)
    priority: int = Field(default=0)
    forceStart: int = Field(default=0, description='is force start 0 NO ,1 YES')
    inQueue: int = Field(default=0, description='ready to get the queue by other task finish 0 NO ,1 YES')
    status: TaskGroupQueueStatus | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["TaskGroupQueue"]
