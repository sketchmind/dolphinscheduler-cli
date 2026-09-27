from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    currentPage: int | None = Field(default=0)

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
EXECUTABLE_SCHEMA_DIGEST = 'sha256:4a9622c58662464acd3e1636a90dbe3fa827d9303f715cc0b3a87a7742217920'
