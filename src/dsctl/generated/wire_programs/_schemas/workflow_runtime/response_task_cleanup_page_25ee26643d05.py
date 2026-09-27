from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class TaskMainInfo(BaseEntityModel):
    id: int = Field(default=0)
    taskName: str | None = Field(default=None)
    taskCode: StrictInt = Field(default=0)
    taskVersion: StrictInt = Field(default=0)
    taskType: str | None = Field(default=None)
    taskCreateTime: str | None = Field(default=None)
    taskUpdateTime: str | None = Field(default=None)
    processDefinitionCode: int = Field(default=0)
    processDefinitionVersion: int = Field(default=0)
    processDefinitionName: str | None = Field(default=None)
    processReleaseState: ReleaseState | None = Field(default=None)
    upstreamTaskMap: dict[int, str | None] | None = Field(default=None)
    upstreamTaskCode: int = Field(default=0)
    upstreamTaskName: str | None = Field(default=None)

class PageInfoTaskMainInfo(PageInfo[TaskMainInfo]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.TaskMainInfo>."""

__all__ = ["ReleaseState", "PageInfo", "TaskMainInfo", "PageInfoTaskMainInfo"]

PageInfo.model_rebuild(_types_namespace=globals())
TaskMainInfo.model_rebuild(_types_namespace=globals())
PageInfo[TaskMainInfo].model_rebuild(_types_namespace=globals())
PageInfoTaskMainInfo.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:55a1d4fb672f913c41f0db417cabe5ba8c7cdaba839bd496e384fd37e4bd1bcb'

RESPONSE_TYPE = PageInfoTaskMainInfo
EXECUTABLE_SCHEMA_DIGEST = 'sha256:25ee26643d05a48ea47dc13056a8e0482d286cf630ecde383d9d799c6430ac84'
