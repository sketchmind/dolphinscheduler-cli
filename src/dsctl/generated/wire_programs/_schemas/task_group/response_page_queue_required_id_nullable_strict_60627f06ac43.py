from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_ffd0a6438e770033ba99756180c7ffcbfe656cf32928b3521bdef9ff6f093a5e import TaskGroupQueueStatus as TaskGroupQueueStatus

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class TaskGroupQueue(BaseEntityModel):
    id: int = Field(default=0)
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

SOURCE_CLOSURE_DIGEST = 'sha256:4f901e24621ec74ee7eedd58984950d9ced99878a999a890619fb7786871e897'

RESPONSE_TYPE = PageInfoTaskGroupQueue
EXECUTABLE_SCHEMA_DIGEST = 'sha256:60627f06ac4381f349b942505800fba3ebf020600ccb3042128dc0ebab9c3905'
