from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field, StrictInt
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
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class PageInfoClusterDto(PageInfo[ClusterDto]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.api.dto.ClusterDto>."""

__all__ = ["ClusterDto", "PageInfo", "PageInfoClusterDto"]

ClusterDto.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[ClusterDto].model_rebuild(_types_namespace=globals())
PageInfoClusterDto.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoClusterDto
EXECUTABLE_SCHEMA_DIGEST = 'sha256:b4a5b47e28eaf5e21e22263b58416e196df04ac574933d0fbba726bba53eb026'
