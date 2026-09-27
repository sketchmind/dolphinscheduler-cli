from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel

T = TypeVar("T")

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class PageInfoMapStringObject(PageInfo[dict[str, object]]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<Map<String, Object>>."""

__all__ = ["PageInfo", "PageInfoMapStringObject"]

PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[dict[str, object]].model_rebuild(_types_namespace=globals())
PageInfoMapStringObject.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoMapStringObject
EXECUTABLE_SCHEMA_DIGEST = 'sha256:8d2540da069da8d142873c31943d807d29a7830d024dc85e5a01e3d6ca735b4c'
