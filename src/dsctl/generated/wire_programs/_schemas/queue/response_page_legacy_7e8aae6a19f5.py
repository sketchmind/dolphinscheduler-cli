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

class Queue(BaseEntityModel):
    id: int = Field(default=0)
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
EXECUTABLE_SCHEMA_DIGEST = 'sha256:7e8aae6a19f54202656cbbd268570028045c74593d15c0fcd4eafb1b1d974c54'
