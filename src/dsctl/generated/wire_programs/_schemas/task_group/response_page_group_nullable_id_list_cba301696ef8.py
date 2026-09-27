from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class TaskGroup(BaseEntityModel):
    id: int | None = Field(default=None)
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
EXECUTABLE_SCHEMA_DIGEST = 'sha256:cba301696ef82684c37a0ffa35286057b71dfc9602576f229e78c8490fa82354'
