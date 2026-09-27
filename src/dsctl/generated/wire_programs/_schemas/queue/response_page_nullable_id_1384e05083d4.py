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
EXECUTABLE_SCHEMA_DIGEST = 'sha256:1384e05083d42b5930017479f5ab218368687f1cbccbe3ca9d7b7cc6d714036d'
