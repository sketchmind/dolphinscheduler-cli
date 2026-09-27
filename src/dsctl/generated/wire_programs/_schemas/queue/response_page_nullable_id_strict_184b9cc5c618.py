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

class Queue(BaseEntityModel):
    id: int | None = Field(default=None)
    queueName: str | None = Field(default=None)
    queue: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

class PageInfoQueue(PageInfo[Queue]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.Queue>."""

__all__ = ["PageInfo", "Queue", "PageInfoQueue"]

PageInfo.model_rebuild(_types_namespace=globals())
Queue.model_rebuild(_types_namespace=globals())
PageInfo[Queue].model_rebuild(_types_namespace=globals())
PageInfoQueue.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoQueue
EXECUTABLE_SCHEMA_DIGEST = 'sha256:184b9cc5c6188cd922e66ebeb5e86bc1263edcb2a84f9ef7ebe0c73fd902cdd4'
