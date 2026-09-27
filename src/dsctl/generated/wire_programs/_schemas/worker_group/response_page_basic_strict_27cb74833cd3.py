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

class WorkerGroup(BaseEntityModel):
    id: int = Field(default=0)
    name: str | None = Field(default=None)
    addrList: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    systemDefault: bool = Field(default=False)

class PageInfoWorkerGroup(PageInfo[WorkerGroup]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.WorkerGroup>."""

__all__ = ["PageInfo", "WorkerGroup", "PageInfoWorkerGroup"]

PageInfo.model_rebuild(_types_namespace=globals())
WorkerGroup.model_rebuild(_types_namespace=globals())
PageInfo[WorkerGroup].model_rebuild(_types_namespace=globals())
PageInfoWorkerGroup.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoWorkerGroup
EXECUTABLE_SCHEMA_DIGEST = 'sha256:27cb74833cd309ae61e6afa5740a04389e800091b7e8a6fa360a02727498aef8'
