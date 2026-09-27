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

class WorkerGroup(BaseEntityModel):
    id: int | None = Field(default=None)
    name: str | None = Field(default=None)
    addrList: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    description: str | None = Field(default=None)
    systemDefault: bool = Field(default=False)
    otherParamsJson: str | None = Field(default=None)

class PageInfoWorkerGroup(PageInfo[WorkerGroup]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.WorkerGroup>."""

__all__ = ["PageInfo", "WorkerGroup", "PageInfoWorkerGroup"]

PageInfo.model_rebuild(_types_namespace=globals())
WorkerGroup.model_rebuild(_types_namespace=globals())
PageInfo[WorkerGroup].model_rebuild(_types_namespace=globals())
PageInfoWorkerGroup.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoWorkerGroup
EXECUTABLE_SCHEMA_DIGEST = 'sha256:20da842fa2d45a3934cac566358e5e1594647349e9c785f5baa0c571632c5d42'
