from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

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
EXECUTABLE_SCHEMA_DIGEST = 'sha256:5c536d71fe6d9fca6883c6aeab286cbdc32a2329e89de36bf3effeaf09c2e4b3'
