from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel

T = TypeVar("T")

class ClusterDto(BaseContractModel):
    id: int = Field(default=0)
    code: int | None = Field(default=None)
    name: str | None = Field(default=None)
    config: str | None = Field(default=None)
    description: str | None = Field(default=None)
    processDefinitions: list[str] | None = Field(default=None)
    operator: int | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class PageInfoClusterDto(PageInfo[ClusterDto]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.api.dto.ClusterDto>."""

__all__ = ["ClusterDto", "PageInfo", "PageInfoClusterDto"]

ClusterDto.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[ClusterDto].model_rebuild(_types_namespace=globals())
PageInfoClusterDto.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoClusterDto
EXECUTABLE_SCHEMA_DIGEST = 'sha256:d43bcf57f7074a752036fa025798011c0893f95b07a813027de5c03e4920eabd'
