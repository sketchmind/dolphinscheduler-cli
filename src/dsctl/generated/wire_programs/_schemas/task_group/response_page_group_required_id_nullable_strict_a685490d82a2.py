from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class TaskGroup(BaseEntityModel):
    id: int = Field(default=0)
    name: str | None = Field(default=None)
    description: str | None = Field(default=None)
    groupSize: int = Field(default=0)
    useSize: int = Field(default=0)
    userId: int = Field(default=0)
    status: int | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    projectCode: int = Field(default=0)

class PageInfoTaskGroup(PageInfo[TaskGroup]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.TaskGroup>."""

__all__ = ["PageInfo", "TaskGroup", "PageInfoTaskGroup"]

PageInfo.model_rebuild(_types_namespace=globals())
TaskGroup.model_rebuild(_types_namespace=globals())
PageInfo[TaskGroup].model_rebuild(_types_namespace=globals())
PageInfoTaskGroup.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoTaskGroup
EXECUTABLE_SCHEMA_DIGEST = 'sha256:a685490d82a273abcdeadf28a97ea2fb6e741af6d69eeafdb03c80825f38ab4b'
