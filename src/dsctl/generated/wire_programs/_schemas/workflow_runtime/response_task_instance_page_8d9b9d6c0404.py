from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel

T = TypeVar("T")

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class PageInfoMapStringObject(PageInfo[dict[str, object]]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<Map<String, Object>>."""

__all__ = ["PageInfo", "PageInfoMapStringObject"]

PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[dict[str, object]].model_rebuild(_types_namespace=globals())
PageInfoMapStringObject.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoMapStringObject
EXECUTABLE_SCHEMA_DIGEST = 'sha256:8d9b9d6c0404f10982cc62d4436e4f04941b50618449de624b11080364574eb4'
