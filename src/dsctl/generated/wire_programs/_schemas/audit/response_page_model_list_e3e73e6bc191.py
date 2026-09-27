from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel

T = TypeVar("T")

class AuditDto(BaseContractModel):
    userName: str | None = Field(default=None)
    modelType: str | None = Field(default=None)
    modelName: str | None = Field(default=None)
    operation: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    description: str | None = Field(default=None)
    detail: str | None = Field(default=None)
    latency: str | None = Field(default=None)

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class PageInfoAuditDto(PageInfo[AuditDto]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.api.dto.AuditDto>."""

__all__ = ["AuditDto", "PageInfo", "PageInfoAuditDto"]

AuditDto.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[AuditDto].model_rebuild(_types_namespace=globals())
PageInfoAuditDto.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoAuditDto
EXECUTABLE_SCHEMA_DIGEST = 'sha256:e3e73e6bc191f99b074ba2273b0fe4eab01153fe0383d88432f8fee40337ef83'
