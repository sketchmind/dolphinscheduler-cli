from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel

T = TypeVar("T")

class EnvironmentDto(BaseContractModel):
    id: int = Field(default=0)
    code: int | None = Field(default=None)
    name: str | None = Field(default=None)
    config: str | None = Field(default=None)
    description: str | None = Field(default=None)
    workerGroups: list[str] | None = Field(default=None)
    operator: int | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class PageInfoEnvironmentDto(PageInfo[EnvironmentDto]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.api.dto.EnvironmentDto>."""

__all__ = ["EnvironmentDto", "PageInfo", "PageInfoEnvironmentDto"]

EnvironmentDto.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[EnvironmentDto].model_rebuild(_types_namespace=globals())
PageInfoEnvironmentDto.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoEnvironmentDto
EXECUTABLE_SCHEMA_DIGEST = 'sha256:e7fd8cdce255852d1b529cf2e763a81a9c6ce6bae9b5d79b0ee2f00226fd4cb4'
