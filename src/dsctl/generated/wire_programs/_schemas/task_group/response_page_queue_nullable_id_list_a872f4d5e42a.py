from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_ffd0a6438e770033ba99756180c7ffcbfe656cf32928b3521bdef9ff6f093a5e import TaskGroupQueueStatus as TaskGroupQueueStatus

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class TaskGroupQueue(BaseEntityModel):
    id: int | None = Field(default=None)
    taskId: int = Field(default=0)
    taskName: str | None = Field(default=None)
    projectName: str | None = Field(default=None)
    projectCode: str | None = Field(default=None)
    processInstanceName: str | None = Field(default=None)
    groupId: int = Field(default=0)
    processId: int = Field(default=0)
    priority: int = Field(default=0)
    forceStart: int = Field(default=0)
    inQueue: int = Field(default=0)
    status: TaskGroupQueueStatus | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

class PageInfoTaskGroupQueue(PageInfo[TaskGroupQueue]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.TaskGroupQueue>."""

__all__ = ["TaskGroupQueueStatus", "PageInfo", "TaskGroupQueue", "PageInfoTaskGroupQueue"]

PageInfo.model_rebuild(_types_namespace=globals())
TaskGroupQueue.model_rebuild(_types_namespace=globals())
PageInfo[TaskGroupQueue].model_rebuild(_types_namespace=globals())
PageInfoTaskGroupQueue.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:7f0693254eb4dda3b7804e233c783fa31482a3241ab451d6fbea09a8c4099c3c'

RESPONSE_TYPE = PageInfoTaskGroupQueue
EXECUTABLE_SCHEMA_DIGEST = 'sha256:a872f4d5e42a272bb7a1e8ee783e86b82d6b23d0f2f1b4ee60735501d7bc669f'
